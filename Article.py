from elasticsearch_dsl import DocType, Date, Integer, Keyword, Text, Completion
from elasticsearch_dsl.connections import connections
import yaml
from elasticsearch_dsl.analysis import CustomAnalyzer as _CustomAnalyzer

with open('config.yml') as ymlfile:
    conf = yaml.load(ymlfile)

es = connections.create_connection(hosts=conf['elasticsearch'])


class CustomAnalyzer(_CustomAnalyzer):
    def get_analysis_definition(self):
        return {}


ik_analyzer = CustomAnalyzer("ik_max_word",filter=['lowercase'])

def gen_suggest(index, info_tuple):
    used_word = set()
    suggests = []
    for text, weight in info_tuple:
        if text:
            #调用es的analyze接口分析字符串
            words = es.indices.analyze(index=index, analyzer="ik_max_word", params={'filter':['lowercase']}, body=text)
            analyzed_words = set([r['token'] for r in words['tokens'] if len(r['token'])>1])
            new_words = analyzed_words - used_word
        else:
            new_words = set()
        if new_words:
            suggests.append({
                'input':list(new_words),
                'weight':weight
            })
    return suggests


class Article(DocType):

    suggest = Completion(analyzer = ik_analyzer)

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

        Article.init()
        article = Article()
        article.meta['id'] = data['id']
        article.title = data['title']
        article.thumbnail = data['thumbnail']
        article.created_at = data['created_at']
        article.content = data['content']
        article.tags = [data['tag']['type']]
        article.suggest = gen_suggest(Article._doc_type.index, ((article.title, 10), (article.content, 7), (article.tags[0], 5)))
        article.save()
