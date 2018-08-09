# -*- coding: utf-8 -*-
import json
import pymysql
import sys
import datetime
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

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
rootCAPath = 'rootCAPath'
certificatePath = 'certificatePath'
privateKeyPath = 'privateKeyPath'
port = 8883
useWebsocket = True
thingName = 'esp8266'
clientId ='esp8266'


logger = logging.getLogger("AWSIoTPythonSDK.core")
logger.setLevel(logging.DEBUG)
streamHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)

# Init AWSIoTMQTTShadowClient
myAWSIoTMQTTShadowClient = None
if useWebsocket:
    myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId)
    myAWSIoTMQTTClient.configureEndpoint(host, port)
    myAWSIoTMQTTClient.configureCredentials(rootCAPath, privateKeyPath, certificatePath)

# AWSIoTMQTTClient connection configuration
myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
myAWSIoTMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec

# Connect to AWS IoT

topic = '$aws/things/esp8266_0FB9EE/shadow/update'
# Create a deviceShadow with persistent subscription
#deviceShadowHandler = myAWSIoTMQTTShadowClient.createShadowHandlerWithName(thingName, True)

# Delete shadow JSON doc
#deviceShadowHandler.shadowDelete(customShadowCallback_Delete, 5)




def lambda_handler(event, context):
    rds_host  = ""
    name = ""
    password = ""
    db_name = ""
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

        message ={'message' : {'text':'계사 1의 현재 평균 온도는 '+str(sensor_data)+'도입니다. 측정 시간은 '+savetime_transform+"입니다.","message_button":{'label':'상세 온도 보기','url':"http://192.168.0.35/google_chart_temperature.php"}} ,'keyboard': {'type':'buttons','buttons':['계사 1 온도', '계사 1 습도', '계사 1 LED ON','계사 1 LED OFF']}}






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

        message ={'message' : {'text':'계사 1의 현재 평균 습도는 '+str(sensor_data)+'% 입니다. 측정 시간은 '+savetime_transform+"입니다.",'message_button':{'label': '상세 습도 보기','url':'http://192.168.0.35/google_chart_humidity.php'}}, 'keyboard': {'type':'buttons','buttons':['계사 1 온도', '계사 1 습도', '계사 1 LED ON','계사 1 LED OFF']}}

    elif(event["content"]=="계사 1 LED ON"):
        myAWSIoTMQTTClient.connect()
        JSONPayload = '{"state":{"desired":{"on":true }}}'
        myAWSIoTMQTTClient.publish(topic, JSONPayload, 1)
        message ={'message' : {'text':'LED를 켰습니다. 직접 가서 확인해보세요.'} ,'keyboard': {'type':'buttons','buttons':['계사 1 온도', '계사 1 습도', '계사 1 LED ON','계사 1 LED OFF']}}
        myAWSIoTMQTTClient.disconnect()


    elif(event["content"]=="계사 1 LED OFF"):
        myAWSIoTMQTTClient.connect()
        JSONPayload = '{"state":{"desired":{"on":false }}}'
        myAWSIoTMQTTClient.publish(topic, JSONPayload, 1)

        message ={'message' : {'text':'LED를 껏습니다. 직접 가서 확인해보세요.'} ,'keyboard': {'type':'buttons','buttons':['계사 1 온도', '계사 1 습도', '계사 1 LED ON','계사 1 LED OFF']}}
        myAWSIoTMQTTClient.disconnect()

    else :
        message ={'message' : {'text':'아직 기능 테스트중입니다.'} ,'keyboard': {'type':'buttons','buttons':['계사 1 온도', '계사 1 습도', '계사 1 LED ON','계사 1 LED OFF']}}



    return message
