import ssl
import time
import paho.mqtt.client as mqttClient
import json

def ssl_alpn():
    try:
        #debug print opnessl version
        ssl_context = ssl.create_default_context()
        ssl_context.set_alpn_protocols([IoT_protocol_name])
        ssl_context.load_verify_locations(cafile=ca)
        ssl_context.load_cert_chain(certfile=cert, keyfile=private)

        return  ssl_context
    except Exception as e:
        print("exception ssl_alpn()")
        raise e


def on_connect(client, userdata, flags, rc):
   if rc == 0:
        print("Connected to Local MQTT broker")

        global Connected                #Use global variable
        Connected = True                #Signal connection 
        client.subscribe("cisco/edge-intelligence/telemetry/#")
        print("Subscribed to Edge Intelligence topic")
   else:
        print("Local MQTT Connection failed")

def on_disconnect(client, userdata, rc):
   print("Local MQTT Client Got Disconnected")

def on_message(client, userdata, message):
    print "EI Message received: "  + message.payload

    topic = message.topic
    print ("EI Topic is: ", topic)

    #Modify incoming message with values for AWS IoT Core
    msg = {}
    msg = json.loads(message.payload)
    msg = json.dumps(msg)
   
    #Publish to AWS IoT Core on incoming EI Topic
    global AWSmqtt
    AWSmqtt.publish(topic, msg)
    print ("Published to AWS")

Connected = False #global variable for the state of the connection

###################################################
#####Connect to Local MQTT Mosquitto Broker

broker_address= "localhost"
port = <MQTT PORT>
user = <MQTT Username>
password = <MQTT Password>

client = mqttClient.Client("MQTTClient")           #create new instance
client.username_pw_set(user, password=password)    #set username and password
client.on_connect= on_connect                      #attach function to callback
client.on_disconnect= on_disconnect                #attach function to callback
client.on_message= on_message

client.connect(broker_address, port=port)  #connect to broker

client.loop_start()                        #start the loop

###################################################
####### Connect to AWS IOT Core ###################

IoT_protocol_name = "x-amzn-mqtt-ca"
aws_iot_endpoint = <AWS Endpoint ARN>
url = "https://{}".format(aws_iot_endpoint)

ca = "AmazonRootCA1.pem"
cert = "XXXXXXX-certificate.pem.crt"
private = "XXXXXXX-private.pem.key"


AWSmqtt = mqttClient.Client("EI-Python")
ssl_context= ssl_alpn()
AWSmqtt.tls_set_context(context=ssl_context)
AWSmqtt.connect(aws_iot_endpoint, port=443)
print "connect to AWS success"

AWSmqtt.loop_start()


###################################################

try:
    while True:
        time.sleep(5)

except KeyboardInterrupt:
    print "exiting"
    client.disconnect()
    client.loop_stop()
    AWSmqtt.disconnect()
    AWSmqtt.loop_stop()
