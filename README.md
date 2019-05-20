# roiimageapi
## Description
This standalone API can process all availabe .tif images for a given polygon with its coordinates as .geojson in background. Those images are then availabe by a simply download link.

## Usage

### Setup Database
Create a Prostgres DB and a user with superuser privileges or at least the privilege to create new databases.
  
### Setup Config File
Set the config with:
1) the database user, 
2) the rootdirectory of the api (here all products will be saved temporary and the all finished images will be found here),
3) the SCI account credentials,
4) the ammount of downloads that can be run at the same time (SCI allows at maximum two per user at the same time, 
    in some cases even 1 download is more efficient than one)
        
### Run the API
When everything is set up you can simply run the api by starting it as a background process.
```
setsid python3 api.py
```
It will automatically create the database if it is not instanciated now and then the api description can be found on 
the configurated port
