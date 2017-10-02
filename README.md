通过Hive查询语句查出结果集，并向Elasticsearch导入的小工具

方便从hive导查询结果到ES的python脚本，采用分页查询机制，结果集合过多时不会撑爆内存


公司的数据分析经常需要看各种报表，多是分析统计类需求，类SQL语言适合做具有筛选逻辑的数据（有时候有的数据无法从业务主库中查出来，只能直接前端埋点），Elasticsearch适合做统计，而且结合Kibana可以直接生成报表！

耗时查询不宜直接在线上查，还好公司已经实现每天在访问低谷期同步线上数据到Hadoop大数据中心。
    
对这类常有的统计类需求，我的做法是先用HQL做筛选逻辑，ES拿到数据再进行聚合统计，如每天、每月、某人的数据。
结合ES其实更多是因为需求方喜欢Kibana的图表。


<br>
脚本使用说明<br>

环境: Python2<br>
命令 #python hive_to_es.py config=<配置文件目录><br>


配置文件使用说明： 使用.ini后缀的配置文件<br>

```ini

# Elasticsearch地址、用户名、密码
[es]
hosts = 192.168.5.200:9200
username = elastic
password = 888888

# Hive地址、端口、数据库名、用户等配置
[hive]
host = 127.0.0.1
port = 10000
authMechanism = PLAIN
user = hive_user
database = dbname

# 自定义job，一个job相当于一个任务，多个job可以同时导多个结果集到ES，定义了job之后，该job的各项配置要单独再给出
[job]
jobs = jobb,joba

[joba]
# 存入ES的目标index和type
es_index = tqc_test
es_type = tqc_test1_type

# 存入ES时，定义一个文档中的各个字段名称，注意与查询结果的各个字段按顺序对应，才能得到对应正确的数据值
# 如该例的HQL为SELECT `name`, `age` FROM student
# ES文档：{"es_name": "xxx", "es_age":12}
columns = es_name,es_age

# HiveQL文件位置
hql_path = ./sql/hql_test.sql

# 存入ES时的分页大小
# 为了防止结果集过大，导致查询时内存吃不消，无分页配置时默认分页大小30000
page_size = 1000

# 导入数据前是否清空该type下所有数据，相当于全量导入结果集，默认没有配置，不会清空原来数据
;overwrite = true

# jobb配置类似如上
[jobb]
es_index = tqc_test
es_type = tqc_test2_type
columns = es_id, es_address
hql_path = ./sql/hql_test_2.sql

page_size = 1000
;overwrite = true


```
