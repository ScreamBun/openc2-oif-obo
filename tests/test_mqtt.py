import time
import paho.mqtt.client as paho

# broker = "localhost"
broker="mosquitto.olympus.mtn" # Local Test Broker
# broker="test.mosquitto.org" # External Test Broker
# broker="35.221.11.97"  # Kevin's laptop

# def on_message(client, userdata, message):
#     time.sleep(1)
#     print("received message =", str(message.payload.decode("utf-8")))

# client= paho.Client("non-Twisted-test")
client = paho.Client("obo-twisted-123456789")

# client.on_message=on_message

print("connecting to broker ")
client.connect(broker)
print("connected ")

client.loop_start()

# print("subscribing ")
# client.subscribe("oc2/cmd/all")
# client.subscribe("house/bulb1")

# time.sleep(2)

print("publishing ")
client.publish("oc2/cmd", "test/testing")
print("published message")

# client.publish("house/bulb1", "test/testing")

# time.sleep(4)

client.disconnect()
client.loop_stop()
