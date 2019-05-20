import json

from PIL import Image
from io import StringIO

import cherrypy as http
from cherrypy.lib import static
import os
import yaml


import database
from observer import Updater

with open("config.yml", "r") as ymlfile:
    config = yaml.load(ymlfile)


ROOT_DIR = os.path.join(os.getcwd(), "data2")
PRODUCT_DIR = os.path.join(ROOT_DIR, "products")
TASK_DIR = os.path.join(ROOT_DIR, "tasks")

import setup
setup.setUpDatabase()
setup.setUpFilepath()

class rest:
    def __init__(self):
        self.updater = Updater()

    @http.expose
    def index(self):
        return """<p>ROI Image Processing API</p>
<h2><span class="sBrace structure-1">/overview</span></h2>
<p><span class="sBrace structure-1">Return HTML-like overview over all taks that are registered</span></p>
<h2><span class="sBrace structure-1">/taskstatus/{taskid}</span></h2>
<p><span class="sBrace structure-1">Returns the current state of the task as a JSON with all its image metadata </span></p>
<h2><span class="sBrace structure-1">/getimage/{taskid}/{imagename}</span></h2>
<p><span class="sBrace structure-1">Downloadlink of the requested image. Its name can be retrieved by requesting the state of the specific task.</span></p>
<h2>/newtask</h2>
<p>With given GeoJson formatted like:</p>
<div class="json" tabindex="-1"><span id="s-1" class="sBrace structure-1"></span>
<div class="json" tabindex="-1"><span id="s-1" class="sBrace structure-1">{ </span><br />&nbsp;&nbsp;&nbsp;<span id="s-2" class="sObjectK">"type"</span><span id="s-3" class="sColon">:</span><span id="s-4" class="sObjectV">"Feature"</span><span id="s-5" class="sComma">,</span><br />&nbsp;&nbsp;&nbsp;<span id="s-6" class="sObjectK">"properties"</span><span id="s-7" class="sColon">:</span><span id="s-8" class="sBrace structure-2">{ </span><br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span id="s-9" class="sObjectK">"taskname"</span><span id="s-10" class="sColon">:</span><span id="s-11" class="sObjectV">"test"</span><span id="s-12" class="sComma">,</span><br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span id="s-13" class="sObjectK">"beginofobservation"</span><span id="s-14" class="sColon">:</span><span id="s-15" class="sObjectV">"1.1.2018"</span><span id="s-16" class="sComma">,</span><br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span id="s-17" class="sObjectK">"endofobservation"</span><span id="s-18" class="sColon">:</span><span id="s-19" class="sObjectV">"13.6.2019"</span><span id="s-20" class="sComma">,</span><br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span id="s-21" class="sObjectK">"band"</span><span id="s-22" class="sColon">:</span><span id="s-23" class="sObjectV">"TCI"</span><br />&nbsp;&nbsp;&nbsp;<span id="s-24" class="sBrace structure-2">}</span><span id="s-25" class="sComma">,</span><br />&nbsp;&nbsp;&nbsp;<span id="s-26" class="sObjectK">"geometry"</span><span id="s-27" class="sColon">:</span><span id="s-28" class="sBrace structure-2">{ </span><br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span id="s-29" class="sObjectK">"type"</span><span id="s-30" class="sColon">:</span><span id="s-31" class="sObjectV">"Polygon"</span><span id="s-32" class="sComma">,</span><br />&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<span id="s-33" class="sObjectK">"coordinates"</span><span id="s-34" class="sColon">:</span><span id="s-35" class="sBracket structure-3">[ ...</span> <span id="s-36" class="sBracket structure-3">]</span><br />&nbsp;&nbsp;&nbsp;<span id="s-37" class="sBrace structure-2">}</span><br /><span id="s-38" class="sBrace structure-1">}</span></div>
</div>
<p><span id="s-31" class="sBrace structure-1"><br />it will add a new task to be processed</span></p>
<h2>/update</h2>
<p>It will start a update routine and check if there are new images and process them.</p>
<h2>/deletetask/{taskid}</h2>
<p>It will delete the task identified by its taskid</p>"""


    @http.expose
    def getimage(self, taskid, imagename):
        path = os.path.join(config["rootdirectory"], "tasks", taskid, imagename+".tif")
        return static.serve_file(path, content_type="image/tif")


    @http.expose
    @http.tools.json_out()
    @http.tools.json_in()
    def newtask(self):
        geojson = http.request.json
        taskid = database.initNewTask(geojson["properties"])
        os.mkdir(os.path.join(config["rootdirectory"], "tasks", str(taskid)))
        TASK_DIR = os.path.join(config["rootdirectory"], "tasks", str(taskid))
        with open(os.path.join(TASK_DIR, "roi.geojson"), "w") as f:
            json.dump(geojson, f)
        #TODO trigger update in background
        return "NEW JOB REGISTERED WITH ID %s" %taskid


    @http.expose
    @http.tools.json_out()
    def taskstatus(self, taskid):
        return database.getTaskStatus(taskid)\

    @http.expose
    @http.tools.json_out()
    def update(self, ):
        self.updater.update()

    @http.expose
    @http.tools.json_out()
    def deletetask(self, taskid):
        database.deleteTask(taskid=taskid)

    @http.expose
    def overview(self):
        tasks = []
        for id in database.getTaskIds():
            tasks.append(database.getTask(id))
        container = """<table width="490">
                    <tbody>
                    <tr>"""
        for task in tasks:
            switcher = {
                "waiting": "silver",
                "finished": "lime",
                "up to date": "teal",
                "working": "gray"
            }
            color = switcher.get(task["status"].strip())
            imageswaiting = len(database.getPendingProduts(taskid=task["task_id"]))
            container += """<table style="background-color: %s;" width="490">
                            <tbody>
                            <tr>
                            <td>Task-Name: %s</td>
                            </tr>
                            <tr>
                            <td><img src="%s" alt="" width="106" height="106" /></td>
                            <td>
                            <table width="329">
                            <tbody>
                            <tr>
                            <td>task-id: %s</td>
                            </tr>
                            <tr>
                            <td>taskstatus: %s</td>
                            </tr>
                            <tr>
                            <td>images finished: %s</td>
                            </tr>
                            <tr>
                            <td>images pending: %s</td>
                            </tr>
                            </tbody>
                            </table>
                            </td>
                            </tr>
                            </tbody>
                            </table>"""%(color, task["task_name"], "localhost:10000/getimage/5/imgage1", task["task_id"], task["status"], task["imagecounter"], imageswaiting, )
            container += """</tr>"""
        container += """</tbody>
                        </table>"""


        return container


http.config.update({'server.socket_host': "0.0.0.0", 'server.socket_port': config["port"]})
http.quickstart(rest())































