from elasticsearch_dsl import DocType, Date, Integer, Keyword, Text
from elasticsearch_dsl.connections import connections
import yaml

with open('config.yml') as ymlfile:
    conf = yaml.load(ymlfile)

#弥补es_dsl代码缺陷
# class CustomAnalyzer(_CustomAnalyzer):
#     def get_analysis_definition(self):
#         return {}
#
#
# ik_analyzer = CustomAnalyzer("ik_max_word",filter=['lowercase'])


class Article(DocType):

    title = Text(analyzer='ik_max_word')
    content = Text(analyzer='ik_max_word')
    tags = Text(analyzer='ik_max_word')
    thumbnail = Keyword()
    created_at = Keyword()

    class Meta:
        index = 'posts'
        doc_type = 'articles'

    def save(self, ** kwargs):
        return super(Article, self).save(** kwargs)

    def store(self, data):
        connections.create_connection(hosts=conf['elasticsearch'])
        Article.init()
        article = Article()
        article.meta['id'] = data['id']
        article.title = data['title']
        article.thumbnail = data['thumbnail']
        article.created_at = data['created_at']
        article.content = data['content']
        article.tags = [data['tag']['type']]
        article.save()
