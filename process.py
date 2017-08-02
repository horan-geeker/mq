import json
from Article import Article

class Process:

    def __init__(self, msg):
        self.msg = json.loads(msg.value.decode('utf-8'))

    def process(self):
        if 'type' in self.msg.keys() and self.msg['type'] == 'elasticsearch':
            self.process_els(self.msg['data'])

    def process_els(self, data):
        print(data)
        article = Article()
        article.store(data)