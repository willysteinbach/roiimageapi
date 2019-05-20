import psycopg2 as psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import yaml
import os


with open("config.yml", "r") as ymlfile:
    config = yaml.load(ymlfile)


# Before use: create db-user "roiimagedb" with password "KDTJ34PAN6" with superuser privileges
def setUpDatabase():
    try:
        conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s"
                                %(config["database"]["databasename"], config["database"]["username"],
                                  config["database"]["host"],config["database"]["password"]))
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        print("Database connected")
        conn.close()
        return True
    except:
            conn = psycopg2.connect("dbname='postgres' user=%s host=%s password=%s"
                                %( config["database"]["username"], config["database"]["host"],
                                   config["database"]["password"]))
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            curs = conn.cursor()
            curs.execute("""CREATE DATABASE %s OWNER %s"""
                         %(config["database"]["databasename"],config["database"]["username"]))
            curs.close()
            conn.close()

            conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s"
                                %(config["database"]["databasename"], config["database"]["username"],
                                  config["database"]["host"],config["database"]["password"]))
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            curs = conn.cursor()
            ddl = """ 
                CREATE TABLE task
                (
                    task_id SERIAL,
                    task_name CHAR(25),
                    creation_date DATE,
                    filepath CHAR(100),
                    observation_begin DATE,
                    observation_end DATE,
                    spectrum CHAR(5),
                    imagecounter INTEGER,
                    status CHAR(10),
                    PRIMARY KEY (task_id)
                );
                CREATE TABLE products
                (
                    product_id SERIAL,
                    product_name CHAR(36),
                    is_processed BOOLEAN,
                    task_id INTEGER,
                    FOREIGN KEY (task_id) REFERENCES task(task_id) ON DELETE CASCADE,
                    PRIMARY KEY (product_id)              
                );
                CREATE TABLE image
                (
                    image_id SERIAL,
                    task_id INTEGER,
                    image_name CHAR(20),
                    date DATE,
                    FOREIGN KEY (task_id) REFERENCES task(task_id) ON DELETE CASCADE,
                    PRIMARY KEY (image_id)
                );
                """
            curs.execute(ddl)
            #print("Database '%s' created and connected to new Database" %config["database"]["databasename"])
            curs.close()
            conn.close()
            return True

def setUpFilepath():
    ROOT_DIR = os.path.join(config["rootdirectory"])
    if not os.path.exists(os.path.join(config["rootdirectory"], "tasks")):
        os.mkdir(os.path.join(config["rootdirectory"], "tasks"))

setUpDatabase()
