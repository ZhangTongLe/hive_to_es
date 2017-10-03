#! /usr/bin/env python
# -*- coding:utf-8 -*-
import codecs
import logging
import time

import pyhs2
from elasticsearch import Elasticsearch
from elasticsearch import helpers as elasticsearch_helper

try:
    # Python3
    import configparser as ConfigParser
except:
    # Python2
    import ConfigParser

import sys
from imp import reload

reload(sys)

# For Python2
sys.setdefaultencoding('utf8')

"""
Created by tangqingchang on 2017-09-02
环境: python2 
python hive_to_es.py config=<配置文件路径>
"""


def get_map(param_list):
    """
    解析键值对形式的参数数组，返回dict
    :param param_list: 参数数组，如sys.argv
    :return:
    """
    param_dict = {}
    for pair in param_list:
        ls = pair.split('=')
        param_dict[ls[0]] = ls[1]
    return param_dict


def get_list(data, f=','):
    """
    分割字符串为数组
    :param data: 字符串
    :param f: 分隔符，默认是','
    :return:
    """
    ls = data.split(f)
    return ls


logging.basicConfig(level=logging.INFO)


def log(*content):
    """
    输出日志
    :param content:
    :return:
    """
    log_content = "[{t}]".format(t=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    for c in content:
        log_content += str(c)
    logging.info(log_content)


def s2t(seconds):
    """
    秒转化为时间字符串
    :param seconds:
    :return:
    """
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "%02d:%02d:%02d" % (h, m, s)


def get_file_content(path):
    """
    读文件
    :param path:
    :return:
    """
    file = codecs.open(path, 'r+', 'utf-8', 'ignore')
    data = file.read()
    file.close()
    return data


def run_hive_query(hql):
    """
    跑hiveQL
    :param hql:
    :return:
    """
    with hive_conn.cursor() as cursor:
        cursor.execute(hql)
        res = cursor.fetchall()
        cursor.close()
        return res


def add_row_number_into_hql(hql):
    """
    拼接为支持分页的HQL
    :param hql:
    :return:
    """
    ql = hql.lstrip()
    start_pos = hql.upper().find("FROM ")
    left = ql[:start_pos]
    right = ql[start_pos:]
    left = left + ",ROW_NUMBER() OVER () AS row_number "
    return "SELECT * FROM(" + left + right + ")t_paging"


def add_paging_limit_into_hql(hql, start_row, to_row):
    """
    拼接为支持分页的HQL，加入分页信息
    :param hql:
    :param start_row:
    :param to_row:
    :return:
    """
    return add_row_number_into_hql(hql) + " WHERE row_number BETWEEN " + str(start_row) + " AND " + str(to_row)


if len(sys.argv) < 2:
    log("参数不足")
    exit(0)

params_dict = get_map(sys.argv[1:])

config = ConfigParser.ConfigParser()
config.readfp(open(params_dict['config'], mode='r+'))
es = Elasticsearch(hosts=get_list(config.get("es", "hosts")),
                   http_auth=(config.get("es", "username"),
                              config.get("es", "password")))

hive_conn = pyhs2.connect(host=config.get("hive", "host"),
                          port=config.get("hive", "port"),
                          authMechanism=config.get("hive", "authMechanism"),
                          user=config.get("hive", "user"),
                          database=config.get("hive", "database"))

DEFAULT_ES_INDEX = config.get("es", "default_index")
MAX_PAGE_SIZE = config.get("paging", "max_page_size")


def run_job(job_config):
    """
     一个任务
    :return:
    """
    log("*************************", job_config['table'], "开始*************************")
    PAGE_SIZE = job_config["page_size"]
    ES_INDEX = job_config["es_index"]
    ES_TYPE = job_config["es_type"]
    ES_COLUMNS = get_list(job_config["columns"])
    OVERWRITE = job_config["overwrite"]

    try:
        HQL_PATH = job_config["hql_path"]
        log("HQL文件: ", HQL_PATH)
        try:
            USER_HQL = get_file_content(HQL_PATH).strip()
        except:
            log("读取HQL文件出错，退出")
            return
    except:
        log("无HQL文件，直接导表数据")
        USER_HQL = "SELECT * FROM " + job_config['table']

    log("ES_INDEX: ", ES_INDEX)
    log("ES_TYPE: ", ES_TYPE)
    log("分页大小: ", PAGE_SIZE)
    log("是否全量：", OVERWRITE)
    log("ES文档各个字段: ", ES_COLUMNS)
    log("原始HQL内容: ", USER_HQL)
    if not (USER_HQL.startswith("select") or USER_HQL.startswith("SELECT")):
        log("只允许SELECT语句, 退出该任务")
        return

    log(">>>>>>>>>>>>>>>初始化结束>>>>>>>>>>>>>>>")

    # 开始记录时间
    start_time = time.time()

    prepare_hql = ("SELECT COUNT(*), MIN(row_number) FROM (" + add_row_number_into_hql(USER_HQL) + ")t_count")
    log("Prepare HQL: ", prepare_hql)
    try:
        log("开始获取总行数和分页起始行...")
        pre_result = run_hive_query(prepare_hql)
        total_count = int(pre_result[0][0])
        if total_count == 0:
            log("数据结果为0，退出该任务")
            return
        current_row_num = int(pre_result[0][1])
    except Exception as e:
        log("获取分页信息HQL执行失败，退出该任务：", e)
        return

    page_count = int((total_count + PAGE_SIZE - 1) / PAGE_SIZE)

    log("结果集合总数: ", total_count)
    log("分页大小: ", PAGE_SIZE)
    log("总页数: ", page_count)
    log("起始行：", current_row_num)

    # es准备
    if es.indices.exists(index=ES_INDEX) is True:
        if OVERWRITE == "true":
            log("全量添加结果集")
            # 删除type下所有数据
            es.delete_by_query(index=ES_INDEX,
                               body={"query": {"match_all": {}}},
                               doc_type=ES_TYPE,
                               params={"conflicts": "proceed"})
        else:
            log("增量添加结果集")
            pass
    else:
        es.indices.create(index=ES_INDEX)
        log("已新创建index：", ES_INDEX)

    # 开始查询
    for p in range(0, page_count):
        log("==================第%s页开始===================" % (p + 1))
        s = time.time()
        log("当前行: ", current_row_num)

        start_row = current_row_num
        to_row = current_row_num + PAGE_SIZE - 1
        log("开始行号: ", start_row)
        log("结束行号: ", to_row)

        final_hql = add_paging_limit_into_hql(USER_HQL, start_row, to_row)

        try:
            log("开始执行: ")
            log(final_hql)
            hive_result = run_hive_query(final_hql)
        except Exception as e:
            log(">>>>>>>>>>>>>>>HQL执行失败，结束该任务：", e, ">>>>>>>>>>>>>>>>>>")
            return

        actions = []
        for r in hive_result:
            _source = {}
            obj = {}
            for i in range(0, len(ES_COLUMNS)):
                _source[ES_COLUMNS[i]] = r[i]
            obj['_index'] = ES_INDEX
            obj['_type'] = ES_TYPE
            obj['_source'] = _source
            actions.append(obj)

        log("开始插入结果到ES...")
        if len(actions) > 0:
            elasticsearch_helper.bulk(es, actions)
        log("插入ES结束...")
        e = time.time()
        log("该页查询时间：", s2t(e - s))
        current_row_num = current_row_num + PAGE_SIZE

    end_time = time.time()
    log("************************", job_config['table'], ": 全部结束，花费时间：", s2t(end_time - start_time),
        "************************")


result_tables = get_list(config.get("table", "tables"))
for result in result_tables:
    job_conf = dict()

    job_conf['table'] = result

    try:
        job_conf['es_index'] = config.get(result, "es_index")
    except:
        job_conf['es_index'] = DEFAULT_ES_INDEX
    try:
        job_conf['page_size'] = int(config.get(result, "page_size"))
    except:
        job_conf['page_size'] = MAX_PAGE_SIZE
    job_conf['page_size'] = min(int(job_conf['page_size']), MAX_PAGE_SIZE)

    try:
        job_conf['overwrite'] = config.get(result, "overwrite")
    except:
        job_conf['overwrite'] = "false"

    try:
        job_conf['hql_path'] = config.get(result, "hql_path")
    except:
        pass

    try:
        job_conf['es_type'] = config.get(result, "es_type")
        job_conf['columns'] = config.get(result, "columns")
    except:
        log(result, "请至少为该结果表配置es_type和columns项，", "跳过该任务")
        continue

    try:
        run_job(job_conf)
    except Exception as e:
        log(result, "执行job出错：", job_conf, ": ", e)
