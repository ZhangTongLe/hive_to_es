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

;Elasticsearch地址、用户名、密码
[es]
hosts = 192.168.2.100:9200
username = elastic
password = 888888

;默认存入的es的index，若结果表没配置es_index，默认存入该index
default_index = tqc_default

;Hive地址、端口、数据库名、用户等配置
[hive]
host = 127.0.0.1
port = 10000
authMechanism = PLAIN
user = hiveuser
database = hivedbname

[paging]
;自定义最大分页大小，也是默认分页大小
max_page_size = 30000

;需要导到ES的各个表的名称；如果是通过HQL筛选新的结果集合再导入ES，结果表名称可自定义
;定义一个结果表相当于开启一个导数据任务，定义了结果表之后，该表的对应的各项配置要在下面单独再给出
[table]
tables = student,result_a,result_b


;例子1：将hive中的student表导入ES，配置头要和hive中的表名称一致
[student]
;可选配置，要导入到ES的哪个index，不配置用默认index
;es_index = tqc_test

;每个导出结果表要至少配置以下两项
;存入Elasticsearch的type
es_type = tqc_type
;存入ES时，定义一个文档中的各个字段名称，注意与查询结果的各个字段按顺序对应，才能得到对应正确的数据值
;以下字段与hive中student表的各个字段对应
columns = es_name,es_age,es_sex,es_home,es_school,es_hobby,es_description

;导出原始数据表时，无需配置HQL文件路径
;hql_path = /sql/test.sql
;为了防止结果集过大，导致查询时内存吃不消，无分页配置时默认分页大小max_page_size
;page_size = 1000
;导入数据前是否清空该type下所有数据，相当于全量导入结果集，默认没有配置，增量导入ES，不会清空原来数据
;overwrite = true

;例子2：通过HQL筛选出结果，选出所有年龄>30的员工，再将结果集合导入ES
[result_a]
es_index = tqc_test_result
es_type = tqc_test1_type

;存入ES时，定义一个文档中的各个字段名称，注意与查询结果的各个字段按顺序对应，才能得到对应正确的数据值
;如该例的HQL为SELECT `id`, `age`, `address` FROM staff WHERE `age`>30
;ES文档为：{"es_id": "123", "es_age":35 , "es_address":"xxxxxxxx"}
columns = es_id,es_age,es_address

;HQL文件位置，目前还不支持带有注释的HQL
hql_path = ./sql/hql_test1.sql
;page_size = 1000
;overwrite = true

;例子3：再定义另一想要导出到ES的结果集合，配置参考例子2
[result_b]
es_index = tqc_test_result
es_type = tqc_test2_type
columns = id,name,age
hql_path = ./sql/hql_test2.sql
page_size = 50
overwrite = true



```
