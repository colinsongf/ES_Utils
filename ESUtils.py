from elasticsearch import Elasticsearch
#from elasticsearch.helpers import bulk
import time
import csv
from os import walk

class ElasticUtils:
    '''
    function 初始化
    :param index_name: 索引名称
    :param index_type: 索引类型
    :param ip: ES的IP地址
    '''
    def __init__(self, index_name, index_type, ip="127.0.0.1"):
        self.index_name = index_name
        self.index_type = index_type
        # 无用户名密码状态
        IP = {"host": ip, "port": 9200}
        self.es = Elasticsearch([IP])
        # 用户名密码状态
        #self.es = Elasticsearch([ip], http_auth=('elastic', 'password'), port=9200)

    '''
    创建索引,创建索引名称为ott，类型为ott_type的索引
    :param ex: Elasticsearch对象
    :return:
    '''
    def create_index(self, index_name="ott", index_type="ott_type"):
        _index_mappings = {
            "mappings": {
                self.index_type: {
                    "properties": {
                        "Question": {
                            "type": "text",
                            "index": True,
                            "analyzer": "ik_max_word",
                            "search_analyzer": "ik_max_word",
                            "similarity": "BM25"
                        },
                        "Answer": {
                            "type": "text",
                            "index": "not_analyzed"
                        }
                    }
                }
            }
        }

        if self.es.indices.exists(index=self.index_name) is not True:
            res = self.es.indices.create(index=self.index_name, body=_index_mappings)
            print(res)

    def Index_Data(self):
        es = Elasticsearch()
        csvdir = '/your_csv_path'
        filenamelist = []
        for (dirpath, dirnames, filenames) in walk(csvdir):
            filenamelist.extend(filenames)
            break
        total = 0
        for file in filenamelist:
            csvfile = csvdir + '/' + file
            self.Index_Data_FromCSV(csvfile, es)
            total += 1
            print(total)
            time.sleep(10)

    '''
    从CSV文件中读取数据，并存储到es中
    :param csvfile: csv文件，包括完整路径
    :return:
    '''
    def Index_Data_FromCSV(self, csvfile):
        with open(csvfile, 'r') as file:
            reader = csv.DictReader(file)
            column = [row for row in reader]

        index = 0
        doc = {}
        for item in column:
            if index > 1:  # 第一行是标题
                doc['Question'] = item['Question']
                doc['Answer'] = item['Answer']
                res = self.es.index(index=self.index_name, doc_type=self.index_type, body=doc)
                print(res['created'])
            index += 1
            print(index)

    def Up_Data(self, data):
        '''
        data = {
            'title': '美国留给伊拉克的是个烂摊子吗',
            'url': 'http://view.news.qq.com/zt2011/usa_iraq/index.htm',
            'date': '2011-12-16'
        }
        '''
        #这里ID需要特别指定
        result = self.es.update(index=self.index_name, doc_type=self.index_type, body=data, id=1)
        print(result)

    """
    通过ElasticSearch从Index中查询文件
    :param input_text:输入的文本, 返回的答案个数
    :return: 查询到的匹配输入文本的答案
    """
    def search(self, input_text, answer_num):
        query = {"query": {"match": {"Question": input_text}}}
        query_doc = self.es.search(body=query, size=answer_num)
        query_doc_source = query_doc["hits"]["hits"]
        query_list = []
        for i in range(len(query_doc_source)):
            query_list.append(query_doc_source[i]["_source"]["Question"])
            query_list.append(query_doc_source[i]["_source"]["Answer"])

        return query_list


'''
    def Index_Data(self):
        
        数据存储到es
        :return:
        
        list = [
            {"date": "2017-09-13",
             "source": "慧聪网",
             "link": "http://info.broadcast.hc360.com/2017/09/130859749974.shtml",
             "keyword": "电视",
             "title": "付费 电视 行业面临的转型和挑战"
             },
            {"date": "2017-09-13",
             "source": "中国文明网",
             "link": "http://www.wenming.cn/xj_pd/yw/201709/t20170913_4421323.shtml",
             "keyword": "电视",
             "title": "电视 专题片《巡视利剑》广获好评：铁腕反腐凝聚党心民心"
             }
        ]
        for item in list:
            res = self.es.index(index=self.index_name, doc_type=self.index_type, body=item)
            print(res['created'])

    def bulk_Index_Data(self):
        
        用bulk将批量数据存储到es
        :return:
        
        list = [
            {"date": "2017-09-13",
             "source": "慧聪网",
             "link": "http://info.broadcast.hc360.com/2017/09/130859749974.shtml",
             "keyword": "电视",
             "title": "付费 电视 行业面临的转型和挑战"
             },
            {"date": "2017-09-13",
             "source": "中国文明网",
             "link": "http://www.wenming.cn/xj_pd/yw/201709/t20170913_4421323.shtml",
             "keyword": "电视",
             "title": "电视 专题片《巡视利剑》广获好评：铁腕反腐凝聚党心民心"
             },
            {"date": "2017-09-13",
             "source": "人民电视",
             "link": "http://tv.people.com.cn/BIG5/n1/2017/0913/c67816-29533981.html",
             "keyword": "电视",
             "title": "中国第21批赴刚果（金）维和部隊启程--人民 电视 --人民网"
             },
            {"date": "2017-09-13",
             "source": "站长之家",
             "link": "http://www.chinaz.com/news/2017/0913/804263.shtml",
             "keyword": "电视",
             "title": "电视 盒子 哪个牌子好？ 吐血奉献三大选购秘笈"
             }
        ]
        ACTIONS = []
        i = 1
        for line in list:
            action = {
                "_index": self.index_name,
                "_type": self.index_type,
                "_id": i,  # _id 也可以默认生成，不赋值
                "_source": {
                    "date": line['date'],
                    "source": line['source'].decode('utf8'),
                    "link": line['link'],
                    "keyword": line['keyword'].decode('utf8'),
                    "title": line['title'].decode('utf8')}
            }
            i += 1
            ACTIONS.append(action)
            # 批量处理
        success, _ = bulk(self.es, ACTIONS, index=self.index_name, raise_on_error=True)
        print('Performed %d actions' % success)
'''



