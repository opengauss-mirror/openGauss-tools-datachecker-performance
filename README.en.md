# gs_datachecker

#### Description
Opengauss data verification tool, including full data verification and incremental data verification.

#### Software Architecture
Full data verification: JDBC is used to extract the source and target data, and the extraction results are temporarily stored in Kafka. The verification service obtains the extraction results of the specified table from Kafka through the extraction service for verification. Finally, output the verification results to the file in the specified path.



Incremental data verification, through debezium monitoring the data change records of the source database, the extraction service regularly processes the change records of debezium according to a certain frequency, and makes statistics on the change records. Send the statistical results to the data verification service. The data verification service initiates incremental data verification and outputs the verification results to the specified path file.

 

**Installation environment requirements:**

```
JDK11+
Install kafka (start zookeeper and kafka service)
```



#### Installation

1.  Download and start Kafka
2.  Obtain the data verification service jar package and the configuration file template (datachecker-check.jar/datachecker-extract.jar, application.yml, application sink.yml, application source.yml)
3.  Copy the jar package and configuration file to the specified server directory, configure the relevant configuration file, and start the corresponding jar service.

#### Instructions

**Start zookeeper**

```
cd {path}/confluent-7.2.0
```

Start the ZooKeeper service

```
bin/zookeeper-server-start etc/kafka/zookeeper.properties
or
bin/zookeeper-server-start -daemon etc/kafka/zookeeper.properties
```

**Start Kafka**

Start the Kafka broker service

```
bin/kafka-server-start  etc/kafka/server.properties

bin/kafka-server-start -daemon etc/kafka/server.properties
```

**Start  kafka connect (incremental check)**

```
# New connect configuration
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

# Start the Kafka connect service 
bin/connect-standalone -daemon etc/kafka/connect-standalone.properties etc/kafka/mysql-conect.properties
```

 **Verify service startup configuration** 

```
Verify the service configuration and modify the application.yml file
	server.port is the verification service web port, which can not be modified by default
	logging.config is the absolute path of the extraction service log path config/log4j2.xml file
	bootstrap-servers is the working address of kafka, and the default installation can not be modified
	data.check.data-path is the output address of the verification result, and the default configuration can not be modified
	data.check.source-uri the source side service request address, and the default configuration can not be modified
	data.check.sink-uri is the service request address of the target end, and the default configuration can not be modified
```

 **Source side service startup configuration** 

```
Source side service configuration modification application-source.yml file
	server.port is the source side extraction service web port, which can not be modified by default
	logging.config is the absolute path of the extraction service log path config/log4j2source.xml file
	spring.check.server-uri is the verification service request address, and the default configuration can not be modified
	spring.extract.schema is the current validation data schema, and the name of the MySQL database
	bootstrap-servers is the working address of kafka, which can not be modified by default
	
	Data Source Configuration
```

 **Target side service startup configuration** 

```
Target side service configuration modification application-sink.yml file
	server.port  is the sink side extraction service web port, which can not be modified by default
	logging.config is the absolute path of the extraction service log path config/log4j2sink.xml file
	spring.check.server-uri is the verification service request address, and the default configuration can not be modified
	spring.extract.schema is the current validation data schema, and the name of the MySQL database
	bootstrap-servers is the working address of kafka, which can not be modified by default
	
	Data Source Configuration
```

**Start datachecker performance service**

```
use extract-endpoints shell command to start the source and sink service
sh extract-endpoints.sh start|stop|restart 
use check-endpoint shell command to start the check service
sh check-endpoint.sh start|stop|restart 

The extraction service must be started first, and then the verification service.
```

 **Background start command** 

```
nohup java  -jar datachecker-extract-0.0.1.jar --source  >/dev/null 2>&1 &

nohup java  -jar datachecker-extract-0.0.1.jar --sink >/dev/null 2>&1 &

nohup java  -jar datachecker-check-0.0.1.jar >/dev/null 2>&1 &
```



**After the verification service is fully started, a verification request will be automatically initiated**

**remarks: **

```
1. Single instance verification uses sh script to start the verification service. If verification needs to be started in parallel, copy the current working directory file. After reconfiguration, use the java background startup command.

2. After the extraction service is started, it will automatically load the table related information of the database. If the data volume is large, the data loading will be time-consuming.

3. After the validation service is started, it will detect whether the table data information on the extraction end has been loaded. If the loading is not completed within a certain period of time, the validation service will automatically exit. At this time, you need to query the table information loading progress of the source and destination, and view the loading progress through the log information. Or restart the verification service directly.

4. The incremental verification service is started, and the source side configuration file  config  application source needs to be modified Debezium enable: true in yml and configure other debezium related configurations. Start the service to start the incremental verification service
```



**Developer local  startup service**

Add virtual machine parameter VM option in startup configuration:

```
Source side extraction service
-Dspring.config.additional-location=.\config\application-source.yml

Destination extraction service
-Dspring.config.additional-location=.\config\application-sink.yml

check service
-Dspring.config.additional-location=.\config\application.yml
```

**Limits and Constraints**

```
JDK version requirements JDK11+
The current version only supports the verification of source MySQL and target openGauss data
The current version only supports data verification, not table object verification
MYSQL requires version 5.7+
The current version does not support the verification of geographic location geometry data
```



#### Contribution

1.  Fork the repository
2.  Create Feat_xxx branch
3.  Commit your code
4.  Create Pull Request


#### Gitee Feature

1.  You can use Readme\_XXX.md to support different languages, such as Readme\_en.md, Readme\_zh.md
2.  Gitee blog [blog.gitee.com](https://blog.gitee.com)
3.  Explore open source project [https://gitee.com/explore](https://gitee.com/explore)
4.  The most valuable open source project [GVP](https://gitee.com/gvp)
5.  The manual of Gitee [https://gitee.com/help](https://gitee.com/help)
6.  The most popular members  [https://gitee.com/gitee-stars/](https://gitee.com/gitee-stars/)

#### Download

[download]: https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/gs_datacheck-5.0.0.tar.gz

