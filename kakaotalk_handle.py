# -*- coding: utf-8 -*-
import json
import pymysql
import sys
import datetime
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
import logging
import time

REGION = 'ap-northeast-1'





def customShadowCallback_Update(payload, responseStatus, token):
    # payload is a JSON string ready to be parsed using json.loads(...)
    # in both Py2.x and Py3.x
    if responseStatus == "timeout":
        print("Update request " + token + " time out!")
    if responseStatus == "accepted":
        payloadDict = json.loads(payload)
        print("~~~~~~~~~~~~~~~~~~~~~~~")
        print("Update request with token: " + token + " accepted!")
        print("on: " + str(payloadDict["state"]["desired"]["on"]))
        print("~~~~~~~~~~~~~~~~~~~~~~~\n\n")
    if responseStatus == "rejected":
        print("Update request " + token + " rejected!")

def customShadowCallback_Delete(payload, responseStatus, token):
    if responseStatus == "timeout":
        print("Delete request " + token + " time out!")
    if responseStatus == "accepted":
        print("~~~~~~~~~~~~~~~~~~~~~~~")
        print("Delete request with token: " + token + " accepted!")
        print("~~~~~~~~~~~~~~~~~~~~~~~\n\n")
    if responseStatus == "rejected":
        print("Delete request " + token + " rejected!")

# Read in command-line parameters

host = 'host'
rootCAPath = 'CAPATH'
port = 8883
useWebsocket = True
thingName = 'thingname'
clientId =''
logger = logging.getLogger("AWSIoTPythonSDK.core")
logger.setLevel(logging.DEBUG)
streamHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

# Init AWSIoTMQTTShadowClient
myAWSIoTMQTTShadowClient = None
if useWebsocket:
    myAWSIoTMQTTShadowClient = AWSIoTMQTTShadowClient(clientId, useWebsocket=True)
    myAWSIoTMQTTShadowClient.configureEndpoint(host, port)
    #myAWSIoTMQTTShadowClient.configureCredentials(rootCAPath)


# AWSIoTMQTTShadowClient configuration
myAWSIoTMQTTShadowClient.configureAutoReconnectBackoffTime(1, 32, 20)
myAWSIoTMQTTShadowClient.configureConnectDisconnectTimeout(10)  # 10 sec
myAWSIoTMQTTShadowClient.configureMQTTOperationTimeout(5)  # 5 sec

# Connect to AWS IoT
myAWSIoTMQTTShadowClient.connect()

# Create a deviceShadow with persistent subscription
deviceShadowHandler = myAWSIoTMQTTShadowClient.createShadowHandlerWithName(thingName, True)

# Delete shadow JSON doc
deviceShadowHandler.shadowDelete(customShadowCallback_Delete, 5)



def lambda_handler(event, context):
    rds_host  = "host"
    name = "name"
    password = "passwd"
    db_name = "DB"
    conn = pymysql.connect(rds_host, user=name, passwd=password, db=db_name)
    curs = conn.cursor(pymysql.cursors.DictCursor)
    # TODO implement
    message ={}

    if(event["content"]=="계사 1 온도"):
        sql_tempmessage="select Sensor_data,savetime from IOT_farm where ID_farm=%s and Sensor_sort=%s ORDER BY savetime DESC"
        curs.execute(sql_tempmessage, (1, 'temp'))
        savetime_row = curs.fetchone()
        rows = curs.fetchmany(5)
        sensor_data = 0
        savetime =''

        for row in rows:
            #print(row['Sensor_data'])
            sensor_data = sensor_data+row['Sensor_data']
            savetime =row['savetime']

        sensor_data = sensor_data/5
        savetime =savetime_row['savetime']
        savetime_transform = savetime.strftime('%Y년 %m월 %d일 %H시 %M분')

        message ={'message' : {'text':'계사 1의 현재 평균 온도는 '+str(sensor_data)+'도입니다. 측정 시간은 '+savetime_transform+"입니다."} ,'keyboard': {'type':'buttons','buttons':['계사 1 온도', '계사 1 습도', '계사 1 LED ON','계사 1 LED OFF']}}






    elif(event["content"]=="계사 1 습도"):
        sql_tempmessage="select Sensor_data,savetime from IOT_farm where ID_farm=%s and Sensor_sort=%s ORDER BY savetime DESC"
        curs.execute(sql_tempmessage, (1, 'humi'))
        savetime_row = curs.fetchone()
        rows = curs.fetchmany(4)

        sensor_data = 0


        for row in rows:
            #print(row['Sensor_data'])
            sensor_data = sensor_data+row['Sensor_data']

        sensor_data = sensor_data/5
        savetime =savetime_row['savetime']
        savetime_transform = savetime.strftime('%Y년 %m월 %d일 %H시 %M분')

        message ={'message' : {'text':'계사 1의 현재 평균 습도는 '+str(sensor_data)+'% 입니다. 측정 시간은 '+savetime_transform+"입니다."} ,'keyboard': {'type':'buttons','buttons':['계사 1 온도', '계사 1 습도', '계사 1 LED ON','계사 1 LED OFF']}}

    elif(event["content"]=="계사 LED ON"):
        JSONPayload = '{"state":{"desired":{"on":true }}}'
        deviceShadowHandler.shadowUpdate(JSONPayload, customShadowCallback_Update, 5)
        message ={'message' : {'text':'LED를 켰습니다. 직접 가서 확인해보세요.'} ,'keyboard': {'type':'buttons','buttons':['계사 1 온도', '계사 1 습도', '계사 1 LED ON','계사 1 LED OFF']}}
    elif(event["content"]=="계사 LED OFF"):
        JSONPayload = '{"state":{"desired":{"on":false }}}'
        deviceShadowHandler.shadowUpdate(JSONPayload, customShadowCallback_Update, 5)
        message ={'message' : {'text':'LED를 껏습니다. 직접 가서 확인해보세요.'} ,'keyboard': {'type':'buttons','buttons':['계사 1 온도', '계사 1 습도', '계사 1 LED ON','계사 1 LED OFF']}}
    else :
        message ={'message' : {'text':'아직 기능 테스트중입니다.'} ,'keyboard': {'type':'buttons','buttons':['계사 1 온도', '계사 1 습도', '계사 1 LED ON','계사 1 LED OFF']}}



    return message
