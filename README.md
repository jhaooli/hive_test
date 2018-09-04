#hive_test

this is a project about hive test. created this project just for learning how to use hive with java api.

hive版本号0.12 hadoop版本号2.7.3

-------------------------------------------------------------------
#1.1 

#一、重构了部分内容

1.url以及driver_name

2.更换了新的测试方式

#这次实验过程：

1.后台开启hive --service metastore &

2.开启远程服务 hive --service hiveserver2

3.url更改为url = "jdbc:hive2://localhost:10000/default"

4.driver_name更改为driverName = "org.apache.hive.jdbc.HiveDriver"

#部分结果如图所示：

Running:show tables 'person'
执行 show tables 运行结果:
person
-------------next----------------


Running:describe person
执行 describe table 运行结果:
id                      int                 
sex                     string              
name                    string              
-------------next----------------


Running:load data local inpath '/opt/people.txt' into table person
-------------next----------------


Running:select * from person
执行 select * query 运行结果:
1       man
2       man
3       man
4       man
5       man
6       women
7       women
8       women
9       women
10      women
-------------next----------------

感悟：建议后面sql引擎换为tez。

------------------------------------------------------------------
#1.0

#this project contains following functions:

1.get connection

2.count data

3.select data

4.load data

5.describe tables

6.show tables

7.create tables

8.drop table