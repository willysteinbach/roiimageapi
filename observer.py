import logging
import shutil
import sys
import threading
import time

import cropping
import database
import os
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt
import yaml
import datetime

with open("config.yml", "r") as ymlfile:
    config = yaml.load(ymlfile)

api = SentinelAPI(config["sciuser"], config["scipassword"], config["scihuburl"], show_progressbars=False)

def checkForNewProducts(taskid):
    task = database.getTask(taskid)
    TASK_DIR = task["filepath"]
    #print(task)
    roi = os.path.join(TASK_DIR, "roi.geojson")
    feature = read_geojson(roi)
    footprint = geojson_to_wkt(feature.geometry)
    #print(footprint)
    begin = datetime.datetime.strptime(task["observation_begin"], "%Y-%m-%d")
    end = datetime.datetime.strptime(task["observation_end"], "%Y-%m-%d")
    if end > datetime.datetime.today():
        #print("query for task: %s" % taskid)

        products = api.query(footprint,
                             date=(begin, datetime.date.today()),
                             platformname='Sentinel-2',
                             cloudcoverpercentage=(0, 30))
        #print(taskid)
        newProductCount = database.updateproducts(taskid, products)
        if len(database.getPendingProduts(taskid=taskid))>0:
            database.setStatus(taskid=taskid, status="waiting")

def unzip_sentinel_product(taskid, title):
    TASK_DIR = os.path.join(config["rootdirectory"], "tasks", str(taskid))
    zip = os.path.join(TASK_DIR, title+".zip")
    import zipfile
    #print(os.listdir(TASK_DIR))
    with zipfile.ZipFile(zip,"r") as zip_ref:
        zip_ref.extractall(TASK_DIR)
    os.rename(os.path.join(TASK_DIR, title+".SAFE"), os.path.join(TASK_DIR, title))

def updateTask(taskid):
    task = database.getTask(taskid)
    products = database.getPendingProduts(taskid)
    database.setStatus(taskid, "working")
    for product in products:
        product_data = api.get_product_odata(product["product_name"])
        #print(product_data)
        for file in os.listdir(task["filepath"]):
            if file.endswith(".zip"):
                #print(file)
                pass
        api.download(product["product_name"], task["filepath"])
        unzip_sentinel_product(taskid, product_data["title"])
        cropping.createimage(taskid, product_data["title"], product_data["date"])
        try:
            os.remove(os.path.join(task["filepath"], product_data["title"] + ".zip"))
        except:
            pass
        try:
            os.remove(os.path.join(task["filepath"], product_data["title"] + ".zip.incomplete"))
        except:
            pass
        shutil.rmtree(os.path.join(task["filepath"], product_data["title"]))
        database.markProductAsProcessed(taskid, product["product_name"])
    if task["observation_end"]<datetime.date.today():
        database.setStatus(taskid, "finished")
    else:
        database.setStatus(taskid, "up to date")


class Updater():
    def __init__(self):
        self.poolsize = config["downloadsatonce"]
        self.isrunning = False
        self.threads = list()

    def update(self):
        if self.isrunning==False:
            self.isrunning= True
        freeSlot = False
        for thread in self.threads:
            if not thread.isAlive():
                freeSlot = True
        if freeSlot: return
        taskids = database.getTaskIds()
        for id in database.getTaskIds():
            checkForNewProducts(id[0])
        while len(taskids)>0:
            if len(self.threads) == 0:
                for i in range(self.poolsize):
                    if len(taskids)>0:
                        self.threads.append(threading.Thread(target=updateTask, args=(taskids[0])))
                        self.threads[len(self.threads)-1].start()
                        taskids = taskids[1:]
                        #print(len(self.threads))
            for thread in self.threads:
                if thread.isAlive()==False:
                    thread = threading.Thread(target=updateTask, args=(taskids[0]))
                    thread.start()
                    taskids = taskids[1:]
            time.sleep(5)
        self.isrunning = False

    def stop(self):
        for thread in self.threads:
            thread.stop()



