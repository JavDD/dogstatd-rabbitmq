import pika
import time
from datadog import initialize, statsd
import json

credentials = pika.PlainCredentials('rmqstatd',':NGg6^RQ_woOhPWg=g')
connection = pika.BlockingConnection(
    pika.ConnectionParameters('prod-rmq02.prediccio.com',5672,'/',credentials,heartbeat=30))
channel = connection.channel()

def callback(ch, method, properties, body):
    
    message = body.decode('utf8').replace("'", '"')
    if message.count('measurement') == 1:
        if is_json(message):
            data = json.loads(message)
            for b in data:
                metric_name = b['measurement']
                metric_value = b['fields']['value']
                tags = b['tags']
                tags_list=[]
                for key,value in tags.items():
                    tags_list.append(f'{key}:{value}')
                
                # tags_value=tags_list.join(',')
                statsd.gauge(metric_name,float(metric_value),tags=tags_list)
                # print(metric_name,float(metric_value),type(tags))
    else:
        messages=message.replace(']','];')
        msgs = messages.split(';')
        for m in msgs:
            if '[{' in m:
                if is_json(m):
                    data = json.loads(m)
                    for b in data:
                        metric_name = b['measurement']
                        metric_value = b['fields']['value']
                        tags = b['tags']
                        tags_list=[]
                        for key,value in tags.items():
                            tags_list.append(f'{key}:{value}')

                        statsd.gauge(metric_name,float(metric_value),tags=tags_list)
                        # print(metric_name,float(metric_value),type(tags))
    


options = {
    'statsd_host':'localhost',
    'statsd_port':8125
}

initialize(**options)

channel.basic_consume(
queue='test-queue-dd', on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C, v-2')



channel.start_consuming()


channel.stop_consuming()

def is_json(myjson):
  try:
    json.loads(myjson)
  except ValueError as e:
    return False
  return True
