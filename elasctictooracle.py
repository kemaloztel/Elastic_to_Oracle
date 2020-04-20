import cx_Oracle
from elasticsearch import Elasticsearch
import datetime
from openpyxl import *

def error_log(error_type):
    username = '*****'           #you can enter your username
    password = '*****'           #you can enter your password
    try:
        print(error_type)
        conn2 = connectOracle(username, password)
        c2 = conn2.cursor()
        sql = "INSERT INTO ELASTICSPSTATS_ERRORLOG (error_type, datee) VALUES (:error_type,:datee)"
        c2.execute(sql, [error_type, datetime.datetime.now()])
        conn2.commit()
        conn2.close()
    except Exception as exc:
        print(exc)

def toExcel(sp, exec, num):
    try:
        kitap = Workbook()
        sheet = kitap.active
        sheet.append(["SP", "EXEC", "DATE"])
        for i in range(num):
            sheet.append((sp[i], exec[i], datetime.datetime.now()))
        kitap.save("C:/Users/Kemal/Documents/sp_exec.xlsx")
        kitap.close()
    except Exception as exc:
        print(exc)

def connectOracle(username, password):
    dsn_tns = cx_Oracle.makedsn('*****', '*****', service_name='*****')
    return cx_Oracle.connect(user=username, password=password, dsn=dsn_tns)

def connectElastic():
    es = Elasticsearch(
        ['*****'],
        scheme="http",
        port=9200,
        http_auth=('admin', '*****'),
        timeout=30
    )
    return es

def readfromElastic_SP():
    try:
        es = connectElastic()
        index = "testvitapp*"

        query = {                   #this is a sample elastic query
            "aggs": {
                "2": {
                    "terms": {
                        "field": "dbcommand.keyword",
                        "order": {
                            "_count": "desc"
                        },
                        "size": 1000000
                    }
                }
            },
            "size": 10000,
            "_source": {
                "excludes": []
            },
            "stored_fields": [
                "*"
            ],
            "script_fields": {},
            "docvalue_fields": [
                {
                    "field": "@timestamp",
                    "format": "date_time"
                }
            ],
            "query": {
                "bool": {
                    "must": [],
                    "filter": [
                        {
                            "match_all": {}
                        },
                        {
                            "range": {
                                "@timestamp": {
                                    "format": "strict_date_optional_time",
                                    "gte": "now-7d",
                                    "lte": "now"
                                }
                            }
                        }
                    ],
                    "should": [],
                    "must_not": []
                }
            }
        }

        resp = es.search(index=index, body=query, scroll='1s')
        source = []
        for x in resp['aggregations']['2']['buckets']:
            source.append(x['key'])
        return source

    except Exception as err:
        error_log(err)

def readfromElastic_EXEC():
    try:
        es = connectElastic()
        index = "testvitapp*"

        query = {
            "aggs": {
                "2": {
                    "terms": {
                        "field": "dbcommand.keyword",
                        "order": {
                            "_count": "desc"
                        },
                        "size": 1000000
                    }
                }
            },
            "size": 10000,
            "_source": {
                "excludes": []
            },
            "stored_fields": [
                "*"
            ],
            "script_fields": {},
            "docvalue_fields": [
                {
                    "field": "@timestamp",
                    "format": "date_time"
                }
            ],
            "query": {
                "bool": {
                    "must": [],
                    "filter": [
                        {
                            "match_all": {}
                        },
                        {
                            "range": {
                                "@timestamp": {
                                    "format": "strict_date_optional_time",
                                    "gte": "now-7d",
                                    "lte": "now"
                                }
                            }
                        }
                    ],
                    "should": [],
                    "must_not": []
                }
            }
        }

        resp = es.search(index=index, body=query, scroll='1s')
        source = []
        for x in resp['aggregations']['2']['buckets']:
            source.append(x['doc_count'])
        return source

    except Exception as error:
        error_log(error)

def writetoOracle():
    username = '*****'
    password = '*****'
    try:
        conn = connectOracle(username, password)
        c = conn.cursor()
        sql = "INSERT INTO ELASTICSPSTATS (sp,exec,datee) VALUES (:sp,:exec,:datee)"
        values_sp = readfromElastic_SP()
        values_exec = readfromElastic_EXEC()
        #print(values_sp[0])
        #print(values_exec[0])
        i = 0
        #print(len(values_sp))
        #print(len(values_exec))
        number = len(values_sp)
        for i in range(number):
            c.execute(sql, [values_sp[i], values_exec[i], datetime.datetime.now()])
        conn.commit()
        conn.close()
        toExcel(values_sp, values_exec, number)
    except Exception as e:
        #print(e)
        error_log(str(e))


if __name__ == '__main__':
    writetoOracle()
