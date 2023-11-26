import pika
import time
from datadog import initialize, statsd
import json
from configparser import ConfigParser
import signal
import logging
from paho.mqtt import client as mqtt_client
import random

logging.basicConfig(filename='/opt/stwin_mqtt/logger.log', encoding='utf-8', format='%(levelname)s - %(asctime)s: %(message)s',datefmt='%Y-%m-%d %H:%M:%S',level=logging.INFO)
logging.info('service has been started')

config = ConfigParser()
config.read('/opt/stwin_mqtt/configuration.ini')

HOST_TYPE = config.get('general_config','HOST_TYPE')

#mqtt host settings
MQTT_HOST = config.get('mqtt_config','MQTT_HOST')
MQTT_PORT = int(config.get('mqtt_config','MQTT_PORT'))
MQTT_USERNAME = config.get('mqtt_config','MQTT_USERNAME')
MQTT_PASSWORD = config.get('mqtt_config','MQTT_PASSWORD')
MQTT_ENV = config.get('mqtt_config','MQTT_ENV')
MQTT_TOPIC = config.get('mqtt_config','MQTT_TOPIC')
MQTT_ERROR_QUEUE = config.get('mqtt_config','MQTT_ERROR_QUEUE')

MQTT_FIRST_RECONNECT_DELAY = 1
MQTT_RECONNECT_RATE = 2
MQTT_MAX_RECONNECT_COUNT = 12
MQTT_MAX_RECONNECT_DELAY = 32

mqtt_connected = False



#rabbitMQ host settings
RABBIT_HOST = config.get('rabbit_config','RABBIT_HOST')
RABBIT_PORT = config.get('rabbit_config','RABBIT_PORT')
RABBIT_VIRTUAL_HOST = config.get('rabbit_config','RABBIT_VIRTUAL_HOST')
RABBIT_USERNAME = config.get('rabbit_config','RABBIT_USERNAME')
RABBIT_PASSWORD = config.get('rabbit_config','RABBIT_PASSWORD')
RABBIT_QUEUES_LIST = config.get('rabbit_config','RABBIT_QUEUES_LIST').split(',')
RABBIT_ENVS_LIST = config.get('rabbit_config','RABBIT_ENVS_LIST').split(',')
RABBIT_ERROR_QUEUE = config.get('rabbit_config','RABBIT_ERROR_QUEUE')
RABBIT_EXCHANGE = config.get('rabbit_config', 'RABBIT_EXCHANGE')
RABBIT_EXCHANGE_TYPE = config.get('rabbit_config', 'RABBIT_EXCHANGE_TYPE')
RABBIT_EXCHANGE_DURABILITY = eval(config.get('rabbit_config', 'RABBIT_EXCHANGE_DURABILITY'))


class GracefulKiller:
  kill_now = False

  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self, *args):
    self.kill_now = True


def is_json_custom(load):
    try:
        json.loads(load)
    except ValueError as e:
        return False
    return True



#DataDog connection
options = {
    'statsd_host':config.get('statsd_config','STATSD_HOST'),
    'statsd_port':config.get('statsd_config','STATSD_PORT'),
    'hostname_from_config': eval(config.get('statsd_config','HOST_NAME_FROM_CONFIG'))
}

logging.info('Connecting to DataDog ...')
initialize(**options)



#RabbitMQ
def rabbit_run():
    try:
        logging.info('Connecting to RabbitMQ  ... ')
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
        channel = connection.channel()
        channel.exchange_declare(exchange=RABBIT_EXCHANGE, exchange_type=RABBIT_EXCHANGE_TYPE, durable= RABBIT_EXCHANGE_DURABILITY)

        for queue in RABBIT_QUEUES_LIST:
            channel.queue_bind(exchange=RABBIT_EXCHANGE, queue=queue, routing_key=queue)
            channel.basic_consume(queue=queue, on_message_callback=on_message_rabbit, auto_ack=True)
            logging.info('Consuming queue (%s) from RabbitMQ (%s)' ,queue,RABBIT_HOST)

        channel.start_consuming()
    except pika.exceptions.StreamLostError as err:
        logging.error("%s. Rabbit StreamLostError. Retrying in 5 seconds...", err)
        print("Rabbit StreamLostError. Retrying in 5 seconds...", err)
        time.sleep(5)

    except Exception as err:
        logging.error("%s. Rabbit connection failed. Retrying in 5 seconds...", err)
        print("Rabbit connection failed. Retrying in 5 seconds...", err)
        #main.mqtt_conected = False
        time.sleep(5)

'''
    try:
        connection.ioloop.start()
    except KeyboardInterrupt:
        logging.warning('Script interrupted manually')
        connection.close()
'''



def on_message_rabbit(ch, method, properties, body):
    try:
        bodymessage = body.decode('utf8').replace("'", '"')

        # *** messages with only one metric ***
        if bodymessage.count('measurement') == 1:
            if is_json_custom(bodymessage):
                data = json.loads(bodymessage)
                for b in data:
                    metric_name = b['measurement']
                    metric_value = b['fields']['value']
                    tags = b['tags']
                    tags_list=[]
                    for key,value in tags.items():
                        tags_list.append(f'{key}:{value}')

                    tags_list.append(f'enviornment:{MQTT_ENV}')
                    # tags_value=tags_list.join(',')
                    statsd.gauge(metric_name,float(metric_value),tags=tags_list)
                    #print(metric_name,float(metric_value),type(tags))
                    print(data)

        else:
            # *** messages with multiples metrics ***
            messages=bodymessage.replace(']','];')
            msgs = messages.split(';')
            for m in msgs:
                if '[{' in m:
                    if is_json_custom(m):
                        data = json.loads(m)
                        for b in data:
                            metric_name = b['measurement']
                            metric_value = b['fields']['value']
                            tags = b['tags']
                            tags_list=[]
                            for key,value in tags.items():
                                tags_list.append(f'{key}:{value}')

                            tags_list.append(f'enviornment:{MQTT_ENV}')
                            statsd.gauge(metric_name,float(metric_value),tags=tags_list)
                            #print(metric_name,float(metric_value),type(tags))
                            print(data)
    except UnicodeDecodeError as e:
        logging.warning(f'Unicode error triggered')
        logging.error(bodymessage)



#classes for MQTT
def connect_mqtt():
    CLIENT_ID = f'P1-{random.randint(0, 1000)}'
    client = mqtt_client.Client(CLIENT_ID)
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect_mqtt
    client.on_message = on_message_mqtt
    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=120)
        client.on_disconnect = on_disconnect_mqtt
        global mqtt_connected
        if not mqtt_connected:
            logging.info("MQTT Connected successfully!")
            print("MQTT Connected successfully!")
            logging.info('Consuming topic (%s) from MQTT broker (%s)', MQTT_TOPIC, MQTT_HOST)
            mqtt_connected = True
        return client

    except Exception as err:
        logging.error("%s. MQTT connection failed. Retrying in 5 seconds...", err)
        print("MQTT connection failed. Retrying in 5 seconds...", err)
        mqtt_connected = False
        time.sleep(5)
        return False

def on_connect_mqtt(client, userdata, flags, rc):
    if rc == 0 and client.is_connected():
        global mqtt_connected
        if not mqtt_connected:
            logging.info("MQTT Connected successfully!")
            print("MQTT Connected successfully!")
            logging.info('Consuming topic (%s) from MQTT broker (%s)', MQTT_TOPIC, MQTT_HOST)

            mqtt_connected = True
        #logging.info('Consuming topic (%s) from MQTT broker (%s)', MQTT_TOPIC, MQTT_HOST)
        client.subscribe(MQTT_TOPIC)
    else:
        print(f'Failed to connect MQTT, return code {rc}')
        logging.error("Failed to connect MQTT, return code %s", rc)
        mqtt_connected = False

def on_disconnect_mqtt(client, userdata, rc):
    logging.warning("MQTT Disconnected with result code: %s", rc)
    print("MQTT Disconnected with result code: ", rc)
    reconnect_count, reconnect_delay = 0, MQTT_FIRST_RECONNECT_DELAY
    while reconnect_count < MQTT_MAX_RECONNECT_COUNT:
        logging.info("MQTT Reconnecting in %s seconds...", reconnect_delay)
        print("MQTT Reconnecting in ", str(reconnect_delay), " seconds...")
        time.sleep(reconnect_delay)

        try:
            client.reconnect()
            logging.info("MQTT Reconnected successfully!")
            logging.info('Consuming topic (%s) from MQTT broker (%s)', MQTT_TOPIC, MQTT_HOST)
            print("MQTT Reconnected successfully!")
            global mqtt_connected
            mqtt_connected = True
            return
        except Exception as err:
            logging.error("%s. MQTT Reconnect failed. Retrying...", err)
            print("MQTT Reconnect failed. Retrying...", err)
            mqtt_connected = False

        reconnect_delay *= MQTT_RECONNECT_RATE
        reconnect_delay = min(reconnect_delay, MQTT_MAX_RECONNECT_DELAY)
        reconnect_count += 1
    logging.critical("MQTT Reconnect failed after %s attempts. Exiting...", reconnect_count)
    print("MQTT Reconnect failed after ",reconnect_count,  " attempts. Exiting..." )


def on_message_mqtt(client, userdata, message):
    try:
        #print(message.payload.decode('utf8'))
        body = message.payload
        bodymessage = body.decode('utf8').replace("'", '"')

        # *** messages with only one metric ***
        if bodymessage.count('measurement') == 1:
            if is_json_custom(bodymessage):
                data = json.loads(bodymessage)
                for b in data:
                    metric_name = b['measurement']
                    metric_value = b['fields']['value']
                    tags = b['tags']
                    tags_list=[]
                    for key,value in tags.items():
                        tags_list.append(f'{key}:{value}')

                    tags_list.append(f'enviornment:{MQTT_ENV}')
                    # tags_value=tags_list.join(',')
                    statsd.gauge(metric_name,float(metric_value),tags=tags_list)
                    print(data)
        else:
            # *** messages with multiples metrics ***
            messages=bodymessage.replace(']','];')
            msgs = messages.split(';')
            for m in msgs:
                if '[{' in m:
                    if is_json_custom(m):
                        data = json.loads(m)
                        for b in data:
                            metric_name = b['measurement']
                            metric_value = b['fields']['value']
                            tags = b['tags']
                            tags_list=[]
                            for key,value in tags.items():
                                tags_list.append(f'{key}:{value}')

                            tags_list.append(f'enviornment:{MQTT_ENV}')
                            statsd.gauge(metric_name,float(metric_value),tags=tags_list)
                            #print(metric_name,float(metric_value),type(tags))
                            print(data)
    except UnicodeDecodeError as e:
        logging.warning(f'Unicode error triggered')
        logging.error(bodymessage)


def mqtt_run():
    logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s',level=logging.DEBUG)
    client = connect_mqtt()
    if client != False:
        client.loop_start()
        time.sleep(1)
        #if client.is_connected():
            #publish(client)
        #else:
        client.loop_stop()
        #client.disconnect()


if __name__ == '__main__':
    killer = GracefulKiller()
    while not killer.kill_now:
        time.sleep(0.01)
        if HOST_TYPE == 'mqtt':
            mqtt_run()
        elif HOST_TYPE == 'rabbit':
            rabbit_run()

logging.info('service has been stopped')
