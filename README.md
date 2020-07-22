# Cisco Edge Intelligence Sample MQTT scripts

Sample code for receiving MQTT data from Edge Intelligence and re-publishing to AWS IOT Core and ThingsBoard.

### 1. AWS IOT Core : EI-MQTT-AWS.py
* X.509 certifcates based connection to AWS IOT Core
	* Place the X.509 certs in a local folder so, the script can use them.
	* Refer to: `https://aws.amazon.com/premiumsupport/knowledge-center/iot-core-publish-mqtt-messages-python/`
* No change needed to incoming EI MQTT Topic or Message Structure

### 2. Thingsboard Dashboard : EI-MQTT-Thingsboard.py
* Custom Auth Token based authentication
	* Refer to `https://thingsboard.io/docs/getting-started-guides/helloworld/`
* Custom MQTT topic in Thingsboard
* Custom MQTT Message structure in Thingsboard

