from process import Process
from pykafka import KafkaClient
import yaml

with open('config.yml') as ymlfile:
    conf = yaml.load(ymlfile)

client = KafkaClient(hosts=conf['kafka'])
topic = client.topics[b'posts']
consumer = topic.get_simple_consumer()
for message in consumer:
    if message is not None:
        p = Process(message)
        try:
            p.process()
        except Exception as e:
            print(e)