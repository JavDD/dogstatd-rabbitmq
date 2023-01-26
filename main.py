import pika
import time
from datadog import initialize, statsd
import json
from configparser import ConfigParser
from amqpstorm import management
import signal
import logging


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

# print(username)
# print(password)
# print(env_lists)

options = {
    'statsd_host':'localhost',
    'statsd_port':8125
}

initialize(**options)

class consume_queue:
    def __init__(self,queue,channel,env):
        self.queue = queue
        self.channel = channel
        self.env = env
        channel.basic_consume(queue=self.queue, on_message_callback=self.callback, auto_ack=True)
        
    def callback(self,ch, method, properties, body):
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
                        # print(metric_name,float(metric_value),type(tags))
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
                                # print(metric_name,float(metric_value),type(tags))
    
    def is_json(self,myjson):
        try:
            json.loads(myjson)
        except ValueError as e:
            return False
        return True

def on_open(connection):
    connection.channel(on_open_callback=on_channel_open)

def on_channel_open(channel):
    global queue_lists,env_lists
    for i,a in enumerate(queue_lists):
        consume_queue(a,channel,env_lists[i])

def on_close(connection):
    connection.channel(on_close_callback=on_channel_close)

def on_channel_close(channel):
    log_channel_close(channel)

credentials = pika.PlainCredentials('rmqstatd',':NGg6^RQ_woOhPWg=g')
connection = pika.SelectConnection(
    pika.ConnectionParameters(str(host),int(port),str(virtual_host),credentials,heartbeat=30),on_open_callback=on_open)

try:
    connection.ioloop.start()
except KeyboardInterrupt:
    logging.warning('Script interrupted manually')
    connection.close()

def log_channel_close(channel):
    logging.warning('Script channel is closed, messages are not logged')
    print('rabbitmq channel is closed')



if __name__ == '__main__':
    killer = GracefulKiller()
    while not killer.kill_now:
        API = management.ManagementApi(f'{host}:{port}', username,
                                    password, verify=True)
        try:
            result = API.aliveness_test(virtual_host)
            if result['status'] == 'ok':
                islive = True
            else:
                logging.warning('RabbitMQ is not alive! :(')
        except management.ApiConnectionError as why:
            logging.warning('Connection Error: %s' % why)
        except management.ApiError as why:
            logging.warning('ApiError: %s' % why)
            
    logging.info('service has been stopped')
