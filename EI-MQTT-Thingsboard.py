######################################################################################
# Description: Sample MQTT python script to recieve MQTT data from Edge Intelligence #
#              and re-publish to ThingsBoard, using custom Auth Token and MQTT Topic.#
# Author: Naveen Manicka (nmanicka@gmail.com)                                        #
######################################################################################

import time
import paho.mqtt.client as mqttClient
import json

def on_connect(client, userdata, flags, rc):
   if rc == 0:
        print("Connected to Local MQTT broker")

        global Connected                #Use global variable
        Connected = True                #Signal connection 
        client.subscribe("cisco/edge-intelligence/telemetry/#")
        print("Subscribed to EI topic")
   else:
        print("Local MQTT Connection failed")

def on_disconnect(client, userdata, rc):
   print("Local MQTT Client Got Disconnected")

def on_message(client, userdata, message):
    print "EI Message received: "  + message.payload

    topic = message.topic
    print ("EI Topic is: ", topic)

    #Load EI MQTT data into dict
    msg = {}
    msg = json.loads(message.payload)

    #Create new dict for Thingsboard
    jdata = {}

    #Create a flat key, value pair of incoming MQTT message
    #Update metric names as required, based on EI data model.
    if "Temperature" in message.payload:
        jdata['Temperature'] = msg["Temperature"]["v"]
    if "Potentiometer" in message.payload:
        jdata['Potentiometer'] = msg["Potentiometer"]["v"]
    if "Vibration" in message.payload:
        jdata['Vibration'] = msg["Vibration"]["v"]

    json_data = json.dumps(jdata)
    print ("TB JSON Data is:", json_data)

    #Publish to Thingsboard device topic 
    Ttopic = "v1/devices/me/telemetry"
    global Tclient
    Tclient.publish(Ttopic, json_data)

Connected = False #global variable for the state of the connection
TConnected = False #global variable for the state of the connection


#### Thingsboard Callbacks

def Ton_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to ThingsBoard broker")

        global TConnected                #Use global variable
        TConnected = True                #Signal connection

    else:
        print("Thingsboard Connection failed")


def Ton_publish(client, userdata, mid):
    print("Thingsboard Message sent with MID:", mid)
    #client.disconnect();

def Ton_disconnect(client, userdata,rc):
    print("Disconnected from Thingsboard with rc:", rc)
    #client.loop_stop()


###################################################
##### Connect to Thingsboard MQTT broker

Tbroker_address= "demo.thingsboard.io"
Tport = 1883
Tuser = "ThingsBoard Auth Token"
Tpassword = ""

Tclient = mqttClient.Client("Thingsboard_MQTT")
Tclient.username_pw_set(Tuser, password=Tpassword)
Tclient.on_connect= Ton_connect
Tclient.on_disconnect= Ton_disconnect
Tclient.on_publish = Ton_publish

Tclient.connect(Tbroker_address, port=Tport)

Tclient.loop_start()

###################################################
#####Connect to Local MQTT Mosquitto Broker

broker_address= "localhost"
port = 5883
user = "eidemo"
password = "cisco"

client = mqttClient.Client("Python")               #create new instance
client.username_pw_set(user, password=password)    #set username and password
client.on_connect= on_connect                      #attach function to callback
client.on_disconnect= on_disconnect                      #attach function to callback
client.on_message= on_message

client.connect(broker_address, port=port)  #connect to broker

client.loop_start()                        #start the loop

###################################################


try:
    while True:
        time.sleep(5)

except KeyboardInterrupt:
    print "exiting"
    client.disconnect()
    client.loop_stop()
    Tclient.disconnect()
    Tclient.loop_stop()

