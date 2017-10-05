<h3>同步hive数据到Elasticsearch的小工具</h3>

可选 全量（默认） 和 增量

同时支持编写SQL产生中间结果表，再导入到ES
<h3>已经支持从impala渠道导数据，极大提升导数据速度</h3>

采用分页查询机制，数据集过多时不会撑爆内存

公司的数据分析、产品、运营经常需要看各种报表，多是分析统计类需求，
Elasticsearch适合做统计分析，结合Kibana可以直接生成报表！

耗时查询不宜直接在线上查，还好公司已经实现每天在访问低谷期同步线上MySQL数据到Hadoop HDFS 大数据中心。
    
对这类常有的统计类需求，我的通常做法是直接导数据表到ES，先用HQL或者ImpalaSQL筛选出结果表，

ES拿到数据再进行聚合统计，如(Date Histogram)每天、每周、每月、某人的数据。

kibana再生成各类可视化图表，最终数据直观展现！

*Elastic官方已经有了Hive integration的同步工具，但是由于使用的hive版本太低，ES又已经是最新版本，
尝试使用hive integration时一直报错，为尽快适应当前需求手动造了该轮子。*

力求简洁的配置，方便使用

<br>
脚本使用说明<br>

环境: Python2 Python3 <br>
命令 #python hive_to_es.py config=<配置文件目录><br>


配置文件使用说明： 使用.ini后缀的配置文件<br>

```ini
;Elasticsearch地址(有多节点，地址用逗号','隔开)、用户名、密码
[es]
hosts = 192.168.3.100:9200
username = elastic
password = 888888

;存入的es的index默认等于hive或impala中的数据库名称
;在这里可配置自定义全局index名，所有导出表将默认导到该index
;default_index = tqc_ttt

;数据平台，默认是hive
;by = impala

;Hive地址、端口、数据库名、用户等配置
[hive]
host = 127.0.0.1
port = 10000
user = hiveuser
auth_mechanism = PLAIN
database = dbname

;Impala地址、端口、数据库名等配置
[impala]
host = 127.0.0.1
port = 21050
database = dbname


;需要导到ES的各个表的名称，同时也是导到ES的type名(可配置)；
;如果是通过SQL筛选出新的结果表再导入ES，结果表名称可自定义，但必须再在下面给出SQL文件路径的配置
[table]
tables = student,score,teacher,my_result_a,my_result_b

;SQL筛选结果表my_result_a
[my_result_a]
;通过编写HQL或ImpalaSQL获得新的结果集表导入ES时的SQL文件路径，目前还不支持带有注释的SQL
sql_path = ./sql/hql_test1.sql

;再定义另一想要导出到ES的结果表
[my_result_b]
sql_path = ./sql/hql_test2.sql


# 如需要对导出表或者结果表作出更多配置，可进行如下可选配置

;配置头为对应要导出的表或结果表的名称
;[student]

;若不使用默认index，则配置此目标index
;es_index = tqc_test
;若不使用默认type，则配置此目标type；默认type与表名一致
;es_type = tqc_test_type

;限定导出的字段
;columns = date,name,age,address,sex

;字段名映射，这里hive表中的name字段映射为ES中的name_in_es，sex字段映射为ES中的sex_in_es...
;column_mapping = date=@timestamp,name=name_in_es,sex=sex_in_es

;where条件语句，导表时限定字段数据值条件
;where = age>20 AND name LIKE 'abc%'

;通过编写HQL或ImpalaSQL获得新的结果集表导入ES时的SQL文件路径，目前还不支持带有注释的SQL
;sql_path = ./sql/hql_test1.sql

;分页查询配置，为了防止一次查询出所有数据，导致结果集过大，内存吃不消，无分页配置时默认分页大小30000
;page_size = 1000

;全量 & 增量：导入数据前是否清空该type下所有数据，默认=true：清空原有type中数据，再把新数据导入ES（全量更新数据）。
;overwrite = false



```
