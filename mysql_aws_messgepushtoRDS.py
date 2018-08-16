# A lambda function to interact with AWS RDS MySQL

import pymysql
import sys
import json

REGION = 'ap-northeast-1'

rds_host  = "rds_host"
name = "name"
password = "password"
db_name = "db_name"

def save_events(event):
    """
    This function fetches content from mysql RDS instance
    """
    result = []
    conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
    curs = conn.cursor()

    print("Received event: " + json.dumps(event, indent=2))
    jsonData = json.loads(json.dumps(event))
    ID_farm =jsonData['state']['reported']['ID_Farm']
    Sensor_sort =jsonData['state']['reported']['Sensor_sort']
    print(ID_farm)
    print(Sensor_sort)
    if(Sensor_sort == "temphumi"):
        temperature =jsonData['state']['reported']['temperature']
        humidity =jsonData['state']['reported']['humidity']
        add_temp = ("INSERT INTO IOT_farm "
               "(ID_farm,Sensor_sort,Sensor_data, Savetime) "
               "VALUES (%s,%s,%s,now())")
        add_humi = ("INSERT INTO IOT_farm "
               "(ID_farm,Sensor_sort,Sensor_data, Savetime) "
               "VALUES (%s,%s,%s,now())")
        curs.execute(add_temp, (ID_farm,'temp',temperature))
        curs.execute(add_humi,(ID_farm,'humi',humidity))
        conn.commit()
        print("temp/humi is inserted to mysql")
    else:
        Sensor_data =jsonData['state']['reported']['Sensor_data']
        add_Sensordata = ("INSERT INTO IOT_farm "
               "(ID_farm,Sensor_sort,Sensor_data, Savetime) "
               "VALUES (%s,%s,%s,now())")
        curs.execute(add_Sensordata, (ID_farm),(Sensor_sort),(Sensor_data))
        conn.commit()
        print(Sensor_data +" is inserted to mysql")





def main(event, context):
    #print("hi")
    save_events(event)



# event = {
#   "id": 777,
#   "name": "appychip"
# }
# context = ""
# main(event, context)
