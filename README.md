# gs_datachecker

#### 介绍
openGauss数据校验工具 ，包含全量数据校验以及增量数据校验。

#### 软件架构
全量数据校验，采用JDBC方式抽取源端和目标端数据，并将抽取结果暂存到kafka中，校验服务通过抽取服务从kafka中获取指定表的抽取结果，进行校验。最后将校验结果输出到指定路径的文件文件中。

增量数据校验，通过debezium监控源端数据库的数据变更记录，抽取服务按照一定的频率定期处理debezium的变更记录，对变更记录进行统计。将统计结果发送给数据校验服务。由数据校验服务发起增量数据校验，并将校验结果输出到指定路径文件。

**安装环境要求：**

	JDK11+
	kafka安装（启动zookeeper，kafka服务）

#### 安装教程

1.  下载并启动kafka
2.  获取数据校验服务jar包，及配置文件模版（datachecker-check.jar/datachecker-extract.jar,application.yml,application-sink.yml,application-source.yml）
3.  将jar包以及配置文件copy到指定服务器目录，并配置相关配置文件，启动相应的jar服务即可。

#### 详细使用说明

**启动Zookeeper**

```
cd {path}/confluent-7.2.0
```

```
bin/zookeeper-server-start  etc/kafka/zookeeper.properties
或者
bin/zookeeper-server-start -daemon etc/kafka/zookeeper.properties
```

**启动Kafka**

```
bin/kafka-server-start etc/kafka/server.properties
或者
bin/kafka-server-start -daemon etc/kafka/server.properties
```

**启动kafka connect（增量校验）**

```
# 新建connect配置
vi etc/kafka/mysql-conect.properties

name=mysql-connect-all
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=
database.port=3306
database.user=root
database.password=test@123
database.server.id=1
database.server.name=mysql_debezium_connect-all
database.whitelist=test
database.history.kafka.bootstrap.servers=
database.history.kafka.topic=mysql_test_topic-all
include.schema.changes=true
transforms=Reroute
transforms.Reroute.type=io.debezium.transforms.ByLogicalTableRouter
transforms.Reroute.topic.regex=(.*)test(.*)
transforms.Reroute.topic.replacement=data_check_test_all

# 启动connect服务
bin/connect-standalone -daemon etc/kafka/connect-standalone.properties etc/kafka/mysql-conect.properties
```

**校验服务启动配置** 

```
校验服务配置 修改application.yml文件
	server.port 为校验服务web端口，默认可不修改
	logging.config  设置校验服务日志路径为config/log4j2.xml文件绝对路径
	bootstrap-servers 为kafka工作地址，默认安装可不修改
	spring.memory-monitor-enable 打印校验进程内存使用情况，默认false,不打印
	spring.check.core-pool-size: 1 并发线程数池设置，最小线程数，可不修改，默认1
    spring.check.maximum-pool-size: 4 并发线程数池设置，最大线程数，可不修改，默认4
    
	data.check.data-path 校验结果输出地址，默认配置可不修改
	data.check.source-uri 源端服务请求地址，默认配置可不修改
	data.check.sink-uri 目标端服务请求地址，默认配置可不修改
	data.check.max-retry-times 心跳等最大尝试次数，默认1000
	data.check.retry-interval-times 心跳、进度等最大间隔时间单位毫秒 10000
    data.check.auto-delete-topic: 配置是否自动删除Topic，0不删除，1校验全部完成后删除，2表校验完成后删除，默认值为2
    data.check.increment-max-diff-count: 配置增量校验最大处理差异记录数，范围[10,5000]
```

**源端服务启动配置**

```
源端服务配置 修改application-source.yml文件
	server.port 为源端抽取服务web端口，默认可不修改
	logging.config 设置校验服务日志路径为config/log4j2source.xml文件绝对路径
	spring.check.server-uri 校验服务请求地址，默认配置可不修改
	spring.extract.schema 当前校验数据schema，mysql 数据库名称
	spring.extract.core-pool-size 并发线程数设置，根据当前环境配置，可不修改
	bootstrap-servers 为kafka工作地址，默认安装可不修改
	
	数据源配置
	工具默认采用druid数据源，用户可以自定义配置连接池参数，可根据当前校验数据库任务数量（表数量）进行调整
	driver-class-name: 数据库驱动名称，可根据源端数据库类型配置，具体见配置文件模板
    url: jdbc连接串，可根据源端数据库类型及库名进行配置，具体见配置文件模板
    username: 源端数据库用户名
    password: 源端数据库密码，需加单引号
    initialSize: jdbc连接池的连接数，默认可不修改
    minIdle: 默认最小连接池数量
    maxActive: 默认激活数据库连接数量
    validationQuery: jdbc保活查询语句，不修改
	
```

**目标端服务启动配置**

```
目标端服务配置 修改application-sink.yml文件
	server.port 为目标端抽取服务web端口，默认可不修改
	logging.config 设置校验服务日志路径为config/log4j2sink.xml文件绝对路径
	spring.check.server-uri 校验服务请求地址，默认配置可不修改
	spring.extract.schema 当前校验数据schema，opengauss schema名称
	spring.extract.core-pool-size 并发线程数设置，根据当前环境配置，可不修改
	bootstrap-servers 为kafka工作地址，默认安装可不修改
	
	数据源配置
	工具默认采用druid数据源，用户可以自定义配置连接池参数，可根据当前校验数据库任务数量（表数量）进行调整
	driver-class-name: 数据库驱动名称，可根据目标端数据库类型配置，具体见配置文件模板
    url: jdbc连接串，可根据目标端数据库类型及库名进行配置，具体见配置文件模板
    username: 目标端数据库用户名
    password: 目标端数据库密码，需加单引号
    initialSize: jdbc连接池的连接数，默认可不修改
    minIdle: 默认最小连接池数量
    maxActive: 默认激活数据库连接数量
```



**启动数据校验服务**

```shell
sh extract-endpoints.sh start|restart|stop
sh check-endpoint.sh start|restart|stop
先启动抽取服务，后启动校验服务。
```

**后台启动命令**

```shell
nohup java -jar datachecker-extract-0.0.1.jar --source  >/dev/null 2>&1 &

nohup java -jar datachecker-extract-0.0.1.jar --sink >/dev/null 2>&1 &

nohup java -jar datachecker-check-0.0.1.jar >/dev/null 2>&1 &
```

**校验服务完全启动成功后，会自动发起校验请求。**

**备注：**

```
1、单实例校验使用sh 脚本启动校验服务，如果需要并行开启校验，复制当前工作目录文件，重新配置后，使用java 后台启动命令。
2、抽取服务在启动后，会自动加载数据库的表相关信息，如果数据量较大，则数据加载会比较耗时。
3、校验服务启动后，会检测抽取端的表数据信息是否加载完成，如果在一定时间内，未完成加载，则校验服务会自行退出。这时需要查询源端和宿端的表信息加载进度，通过日志信息查看加载进度。或者直接重新启动校验服务。
4、增量校验服务启动，需要修改源端配置文件\config\application-source.yml 中	debezium-enable:true并配置其他 debezium相关配置，服务启动即可开启增量校验服务
```

**开发人员本地 启动服务**

在启动配置中添加虚拟机参数 VM Option : 

```
源端抽取服务
-Dspring.config.additional-location=.\config\application-source.yml

宿端抽取服务
-Dspring.config.additional-location=.\config\application-sink.yml

校验服务
-Dspring.config.additional-location=.\config\application.yml
```

**限制与约束**

```
JDK版本要求JDK11+
当前版本仅支持对源端是MySQL或openGauss，目标端也是MySQL或openGauss数据校验
当前版本仅支持数据校验，不支持表对象校验
MYSQL需要5.7+版本
当前版本地理位置几何图形只支持openGauss到openGauss的数据校验，bit类型不支持openGauss到MySQL的校验
```



#### 参与贡献

1.  Fork 本仓库
2.  新建 Feat_xxx 分支
3.  提交代码
4.  新建 Pull Request


#### 下载地址

[下载链接]: https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/gs_datacheck-5.0.0.tar.gz

