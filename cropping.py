import fnmatch
import json
import uuid

import geopandas
import rasterio
from rasterio import mask
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
import os
from geojson import FeatureCollection
import yaml
from rasterio import plot
import fiona
import database

with open("config.yml", "r") as ymlfile:
    config = yaml.load(ymlfile)


def create_bb_data_frame(left, bottom, right, top):
    """
    returns a geopandas.GeoDataFrame bounding box
    """
    from shapely.geometry import Point, Polygon

    p1 = Point(left, top)
    p2 = Point(right, top)
    p3 = Point(right, bottom)
    p4 = Point(left, bottom)

    np1 = (p1.coords.xy[0][0], p1.coords.xy[1][0])
    np2 = (p2.coords.xy[0][0], p2.coords.xy[1][0])
    np3 = (p3.coords.xy[0][0], p3.coords.xy[1][0])
    np4 = (p4.coords.xy[0][0], p4.coords.xy[1][0])

    bb_polygon = Polygon([np1, np2, np3, np4])

    return geopandas.GeoDataFrame(geopandas.GeoSeries(bb_polygon), columns=['geometry'])


def createimage(taskid, producttitle, dat):
    PRODUCT_DIR = os.path.join(config["rootdirectory"], "tasks",str(taskid), str(producttitle))
    feature = read_geojson(os.path.join(config["rootdirectory"], "tasks", str(taskid), "roi.geojson"))

    geom = geopandas.GeoDataFrame.from_features(FeatureCollection([feature]))
    geom.crs = fiona.crs.from_epsg(4326)

    for root, dir_names, file_names in os.walk(os.path.join(config["rootdirectory"], "tasks", str(taskid), producttitle)):
        sorted_files = sorted(fnmatch.filter(file_names, "*.jp2"))
        filename = fnmatch.filter(file_names, "*"+feature["properties"]["band"]+"_10m.jp2")
        if len(filename) == 0:
            continue
        with rasterio.open(os.path.join(root, filename[0])) as band:
            projected_geom = geom.to_crs(band.crs)

            roi_bb = create_bb_data_frame(projected_geom.bounds.minx, projected_geom.bounds.miny,
                                          projected_geom.bounds.maxx, projected_geom.bounds.maxy)

            roi_bb_polygons = list(
                map(lambda item: json.loads(geopandas.GeoSeries(item).to_json())["features"][0]["geometry"],
                    roi_bb.geometry))

            bb_mask, bb_transform = mask.mask(band, roi_bb_polygons, crop=True)
            plot.show(bb_mask)

            profile = band.meta.copy()
            profile.update({"driver": "GTIFF",
                            "dtype": bb_mask.dtype,
                            "height": bb_mask.shape[1],
                            "width": bb_mask.shape[2],
                            "transform": bb_transform})
            img_name = "image"+ str((database.getImageCounter(taskid)+1)) + ".tif"
            img_file = os.path.join(config["rootdirectory"], "tasks", str(taskid), img_name)
            with rasterio.open(img_file, "w", **profile) as dst:
                dst.write(bb_mask)
            database.registerNewImage(taskid, img_name, dat)
