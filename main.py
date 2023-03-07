import pika
import time
from datadog import initialize, statsd
import json
from configparser import ConfigParser
from amqpstorm import management
import signal
import logging
import csv
from csv import writer
from datetime import timedelta, datetime
import paho.mqtt.client as mqtt


class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self, *args):
    self.kill_now = True

logging.basicConfig(filename='logger.log', encoding='utf-8', format='%(levelname)s - %(asctime)s: %(message)s',datefmt='%Y-%m-%d %H:%M:%S',level=logging.INFO)

logging.info('service has been started')

config = ConfigParser()
config.read('configuration.ini')

host = config.get('variables','host')
port = config.get('variables','port')
virtual_host = config.get('variables','virtual_host')

queue_lists = config.get('queues','names').split(',')
# print(queue_lists)

env_lists = config.get('queues','env').split(',')
username = config.get('credentials','username')
password = config.get('credentials','password')
error_queue = config.get('error queue','name')

mqtt_url = config.get('mqtt queue','url')
mqtt_queue = config.get('mqtt queue','queue')
mqtt_allow = config.get('mqtt queue','mqtt')
mqtt_port = config.get('mqtt queue','port')
mqtt_errorqueue = config.get('mqtt queue','errorqueue')
mqtt_env = config.get('mqtt queue','env')
# print(username)
# print(password)
# print(env_lists)


options = {
    'statsd_host':'localhost',
    'statsd_port':8125
}

initialize(**options)

class consume_queue:
    def __init__(self,queue,channel,env,error_queue):
        self.queue = queue
        self.channel = channel
        self.env = env
        self.error_queue = error_queue
        channel.basic_consume(queue=self.queue, on_message_callback=self.callback, auto_ack=True)
        
    def callback(self,ch, method, properties, body):
        try:
            message = body.decode('utf8').replace("'", '"')
            if message.count('measurement') == 1:
                if self.is_json(message):
                    data = json.loads(message)
                    for b in data:
                        metric_name = b['measurement']
                        metric_value = b['fields']['value']
                        tags = b['tags']
                        tags_list=[]
                        for key,value in tags.items():
                            tags_list.append(f'{key}:{value}')
                        
                        tags_list.append(f'enviornment:{self.env}')
                        # tags_value=tags_list.join(',')
                        statsd.gauge(metric_name,float(metric_value),tags=tags_list)
                        #print(metric_name,float(metric_value),type(tags))
                        #print(data)
            else:
                messages=message.replace(']','];')
                msgs = messages.split(';')
                for m in msgs:
                    if '[{' in m:
                        if self.is_json(m):
                            data = json.loads(m)
                            for b in data:
                                metric_name = b['measurement']
                                metric_value = b['fields']['value']
                                tags = b['tags']
                                tags_list=[]
                                for key,value in tags.items():
                                    tags_list.append(f'{key}:{value}')

                                tags_list.append(f'enviornment:{self.env}')
                                statsd.gauge(metric_name,float(metric_value),tags=tags_list)
                                #print(metric_name,float(metric_value),type(tags))
                                #print(data)
        except UnicodeDecodeError as e:
            logging.warn(f'Unicode error triggered, to be sent to {self.error_queue}')
            with open('error.csv', 'a') as f_object:
                new_row = [datetime.now(),body]
                writer_object = csv.writer(f_object)
                writer_object.writerow(new_row)


    
    def is_json(self,myjson):
        try:
            json.loads(myjson)
        except ValueError as e:
            return False
        return True
    
    def delete_first_n_rows(self,FILENAME,n):
        totalRecords = 0
        with open(FILENAME) as f:
            data = f.read().splitlines()
            totalRecords = len(data)
        if totalRecords > 1000:
            with open(FILENAME, 'w') as g:
                g.write('\n'.join([data[:n]] + data[n+1:]))

def on_open(connection):
    connection.channel(on_open_callback=on_channel_open)

def on_channel_open(channel):
    global queue_lists,env_lists
    for i,a in enumerate(queue_lists):
        consume_queue(a,channel,env_lists[i],error_queue)

def on_close(connection):
    connection.channel(on_close_callback=on_channel_close)

def on_channel_close(channel):
    log_channel_close(channel)

def on_message_mqtt(client, userdata, message):
    try:
        #print(message.payload.decode('utf8'))
        body = message.payload
        bodymessage = body.decode('utf8').replace("'", '"')
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
                        
                    tags_list.append(f'enviornment:{mqtt_env}')
                    # tags_value=tags_list.join(',')
                    statsd.gauge(metric_name,float(metric_value),tags=tags_list)
                    #print(metric_name,float(metric_value),type(tags))
                    #print(data)
        else:
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

                            tags_list.append(f'enviornment:{mqtt_env}')
                            statsd.gauge(metric_name,float(metric_value),tags=tags_list)
                            #print(metric_name,float(metric_value),type(tags))
                            #print(data)
    except UnicodeDecodeError as e:
        logging.warn(f'Unicode error triggered, to be sent to ')
        with open('error.csv', 'a') as f_object:
            new_row = [datetime.now(),message.payload]
            writer_object = csv.writer(f_object)
            writer_object.writerow(new_row)
    #print("received message: " ,str(message.payload.decode("utf-8")))

def is_json_custom(load):
    try:
        json.loads(load)
    except ValueError as e:
        return False
    return True

if mqtt_allow!='True':
    credentials = pika.PlainCredentials(username,password)

    error_csv= csv.reader(open('error.csv'))
    if len(list(error_csv))>0:
        connection_error = pika.SelectConnection(
            pika.ConnectionParameters(str(host),int(port),str(virtual_host),credentials,heartbeat=30))

        channel_error = connection_error.channel()
        if error_queue !='':
            lines = list()
            channel_error.queue_declare(queue=error_queue)
            for row in error_csv:
                channel_error.basic_publish(exchange='', routing_key='Error messages', body=row[1])
                with open('error.csv', 'r') as readFile:
                    reader = csv.reader(readFile)
                    for row in reader:
                        lines.append(row)
                        for field in row:
                            if field == row[1]:
                                lines.remove(row)

                with open('error.csv', 'w') as writeFile:
                    writer = csv.writer(writeFile)
                    writer.writerows(lines)

        connection_error.close()



    connection = pika.SelectConnection(
        pika.ConnectionParameters(str(host),int(port),str(virtual_host),credentials,heartbeat=30),on_open_callback=on_open)

    try:
        connection.ioloop.start()
    except KeyboardInterrupt:
        logging.warning('Script interrupted manually')
        connection.close()

else:
    try:
        # mqtt_queue =mqtt_queue.split(',')
        error_csv= csv.reader(open('error.csv'))
        if len(list(error_csv))>0:
            client_name="P2"
            client=mqtt.Client(client_name)
            client.connect(mqtt_url)
            client.on_message=on_message_mqtt
            client.loop_start()

            
            if error_queue !='':
                lines = list()
                for row in error_csv:
                    client.publish(mqtt_errorqueue,f'Error with format, message: {row[1]}',qos=0,retain=True)
                    with open('error.csv', 'r') as readFile:
                        reader = csv.reader(readFile)
                        for row in reader:
                            lines.append(row)
                            for field in row:
                                if field == row[1]:
                                    lines.remove(row)

                    with open('error.csv', 'w') as writeFile:
                        writer = csv.writer(writeFile)
                        writer.writerows(lines)
            client.loop_stop()
        while True:
            #print(mqtt_url,mqtt_port,mqtt_queue)
            client_name="P1"
            
            client=mqtt.Client(client_name)
            client.connect(mqtt_url)
            client.on_message=on_message_mqtt
            client.loop_start()

            client.subscribe(mqtt_queue)
            
            time.sleep(1)
            client.loop_stop()
    except KeyboardInterrupt:
        logging.warning('Script interrupted manually')

def log_channel_close(channel):
    logging.warning('Script channel is closed, messages are not logged')
    print('rabbitmq channel is closed')



if __name__ == '__main__':
    killer = GracefulKiller()
    while not killer.kill_now:
        time.sleep(0.1)
        # API = management.ManagementApi(f'{host}:{port}', username,
        #                             password, verify=True)
        # try:
        #     result = API.aliveness_test(virtual_host)
        #     if result['status'] == 'ok':
        #         islive = True
        #     else:
        #         logging.warning('RabbitMQ is not alive! :(')
        # except management.ApiConnectionError as why:
        #     logging.warning('Connection Error: %s' % why)
        # except management.ApiError as why:
        #     logging.warning('ApiError: %s' % why)
            
    logging.info('service has been stopped')


