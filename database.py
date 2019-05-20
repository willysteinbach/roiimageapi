import psycopg2 as psycopg2
from bson import json_util
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import yaml
import datetime
import json
from django.core.serializers.json import DjangoJSONEncoder
from psycopg2.extras import RealDictCursor

with open("config.yml", "r") as ymlfile:
    config = yaml.load(ymlfile)

def executeStatement(statement):
    conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s"
                            % (config["database"]["databasename"], config["database"]["username"],
                               config["database"]["host"], config["database"]["password"]))
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    curs = conn.cursor()
    curs.execute(statement)
    try:
        retrows = curs.fetchall()
        curs.close()
        conn.close()
        return retrows
    except:
        curs.close()
        conn.close()
        return []



def initNewTask(properties):
    #print(properties)
    #print(str(properties["taskname"]),str(datetime.datetime.today()),str(properties["beginofobservation"]),
                  #str(properties["endofobservation"]),str(properties["band"]), str(0), str(False))
    statement = """INSERT INTO task (%s) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s') RETURNING task_id """ \
                %("task_name, creation_date, observation_begin, observation_end, spectrum, imagecounter, status",
                  str(properties["taskname"]),str(datetime.datetime.today()),str(properties["beginofobservation"]),
                  str(properties["endofobservation"]),str(properties["band"]), str(0), str(False))
    taskid = executeStatement(statement)[0][0]
    statement = """UPDATE task SET filepath = '%s' WHERE task_id=%s"""%(config["rootdirectory"]+"tasks/"+str(taskid)+"/",taskid)
    executeStatement(statement)
    return taskid

def getTaskPath(taskid):
    statement = """SELECT filepath FROM task WHERE task_id=%s"""%taskid
    #print("Got taskpath from database: %s")%executeStatement(statement)
    return executeStatement(statement)

def getTask(taskid):
    statement = """SELECT * FROM task WHERE task_id=%s"""%taskid
    conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s"
                            % (config["database"]["databasename"], config["database"]["username"],
                               config["database"]["host"], config["database"]["password"]))
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    curs = conn.cursor(cursor_factory=RealDictCursor)
    curs.execute(statement)
    retjson = json.loads(json.dumps(curs.fetchall(), cls=DjangoJSONEncoder))[0]
    retjson["filepath"]= retjson["filepath"].strip()
    curs.close()
    conn.close()
    return retjson

def getPendingProduts(taskid):
    statement = """SELECT * FROM products WHERE task_id=%s AND is_processed = false""" % (taskid)
    conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s"
                            % (config["database"]["databasename"], config["database"]["username"],
                               config["database"]["host"], config["database"]["password"]))
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    curs = conn.cursor(cursor_factory=RealDictCursor)
    curs.execute(statement)
    retjson = json.loads(json.dumps(curs.fetchall(), cls=DjangoJSONEncoder))
    #print(retjson)
    curs.close()
    conn.close()
    return retjson


def getTaskStatus(taskid):
    statement = """SELECT imagecounter, status, task_id FROM task WHERE task_id=%s"""%taskid
    ret = executeStatement(statement)
    status = {
        "task_id": ret[0][2],
        "number_of_images_processed": ret[0][0],
        "status": ret[0][1],
        "available_images": []
    }
    statement = """SELECT image_id, date, image_name FROM image WHERE task_id=%s"""%taskid
    images = executeStatement(statement)
    for image in images:
        img = {
            "image_id": image[0],
            "date": str(image[1]),
            "imagename" : image[2]
        }
        status["available_images"].append(img)
    return status

def getTaskIds():
    statement = """SELECT task_id FROM task WHERE task_id >= 0"""
    ids = executeStatement(statement)
    return ids

def checkiffinished(taskid):
    statement = """SELECT is_finished FROM task WHERE task_id=%s"""%taskid
    ret = executeStatement(statement)
    return ret[0][0]


def getImageCounter(taskid):
    statement = """SELECT imagecounter FROM task WHERE task_id=%s"""%taskid
    ret = executeStatement(statement)
    return ret[0][0]

def registerNewImage(taskid, imagename, date):
    statement = """INSERT INTO image (%s) VALUES ('%s', '%s', '%s')"""%("task_id, image_name, date", taskid, imagename, date)
    executeStatement(statement)
    imgCount = getImageCounter(taskid)
    statement = """UPDATE task SET imagecounter = '%s' WHERE task_id=%s"""%((imgCount+1),taskid)
    executeStatement(statement)

def markfinishedtask(taskid):
    statement = """UPDATE task SET is_finished = 'True' WHERE task_id=%s"""%taskid
    executeStatement(statement)

def updateproducts(taskid, products):
    updatecounter = 0
    for product in products:
        statement = """SELECT product_id FROM products WHERE product_name = '%s' AND task_id = %s"""%(str(product), str(taskid))
        if len(executeStatement(statement))==0:
            statement = """INSERT INTO products (product_name, is_processed, task_id) VALUES ('%s', '%s', '%s')"""%( str(product), str(False), str(taskid))
            updatecounter += 1
            executeStatement(statement)
    return updatecounter

def markProductAsProcessed(taskid, product_name):
    statement = """UPDATE products SET is_processed = TRUE WHERE task_id = %s AND product_name ='%s'""" %(str(taskid), str(product_name))
    executeStatement(statement)

def deleteTask(taskid):
    statement = """DELETE FROM task WHERE task_id=%s"""%taskid
    executeStatement(statement)

def setStatus(taskid, status):
    statement = """UPDATE task SET status = '%s' WHERE task_id = %s"""%(status,taskid)
    executeStatement(statement)
