# gs_datachecker

#### Overview
This tool is used to verify openGauss data, including full data verification and incremental data verification.

#### Software Architecture
Full data verification: Source and destination data are extracted via JDBC and temporarily stored in Kafka. The verification service retrieves specific table data from Kafka, validates it, and exports the results to a specified file.

Incremental data verification: Debezium monitors source database changes. The extraction service periodically processes these records for statistics, sends them to the verification service, which then performs the check and exports the results to a specified file.

#### Downloading the Installation Package

~~~
wget: https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/gs_datacheck-7.0.0rc2.tar.gz
~~~

#### Performing Integrity Check

To ensure the software package is not corrupted during transmission, for example, due to network fluctuations or storage media issues, you must verify its integrity. Only packages that pass this verification can be deployed. Follow the steps below to proceed:

1. Calculate the SHA256 value of the downloaded package. (The following uses `gs_datacheck_6.0.0` as an example. The operations for other versions are the same.)

~~~
sha256sum gs_datacheck-6.0.0.tar.gz
~~~

2. Copy the SHA256 value of the corresponding software package in the openGauss Tools section on the [official website](https://opengauss.org/en/download/). Compare it with the SHA256 value calculated in step 1. If they are consistent, the downloaded package is complete. Otherwise, download the package again.

**Installation Environment Requirements**

	JDK 11+
	Kafka installed (including ZooKeeper and Kafka services)

#### Installation

1. Download and start Kafka.
2. Obtain the JAR package of the data verification service and the configuration file templates: `datachecker-check.jar`, `datachecker-extract.jar`, `application.yml`, `application-sink.yml`, and `application-source.yml`.
3. Copy the JAR packages and configuration files to the specified server directory, configure the relevant files, and start the corresponding services.

#### Usage Description

**Starting ZooKeeper**

```
cd {path}/confluent-7.2.0
```

```
bin/zookeeper-server-start  etc/kafka/zookeeper.properties
Or:
bin/zookeeper-server-start -daemon etc/kafka/zookeeper.properties
```

**Starting Kafka**

```
bin/kafka-server-start etc/kafka/server.properties
Or:
bin/kafka-server-start -daemon etc/kafka/server.properties
```

**Starting Kafka Connect (Incremental Verification)**

```
# Create Connect configurations.
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

# Start the Connect service.
bin/connect-standalone -daemon etc/kafka/connect-standalone.properties etc/kafka/mysql-conect.properties
```

**Configuring the Verification Service**

```
Modify the `application.yml` file.
	server.port: The web port of the verification service. You can retain the default value.
	logging.config: Set the log path of the verification service to the absolute path of the config/log4j2.xml file.
	bootstrap-servers: The Kafka working address. You can retain the default value.
	spring.memory-monitor-enable: Whether to print the memory usage of the verification process. The default value is false, indicating that the memory usage is not printed.
	spring.check.core-pool-size: 1: The minimum number of threads in the concurrent thread pool. You can retain the default value 1.
    spring.check.maximum-pool-size: 4: The maximum number of threads in the concurrent thread pool. You can retain the default value 4.
    
	data.check.data-path: The output address of the verification result. You can retain the default value.
	data.check.source-uri: The request address of the source service. You can retain the default value.
	data.check.sink-uri: The request address of the destination service. You can retain the default value.
	data.check.max-retry-times: The maximum number of heartbeat attempts. The default value is 1,000.
	data.check.retry-interval-times: The maximum interval for heartbeat and progress, in milliseconds. The default value is 10,000.
    data.check.auto-delete-topic: Whether to automatically delete topics. 0: No; 1: Yes; 2 (default): Yes after the verification.
    data.check.increment-max-diff-count: The maximum number of differential records for incremental verification. The value range is [10, 5000].
```

**Configuring Service Startup on the Source End**

```
To configure the source service, modify the `application-source.yml` file.
	server.port: The web port of the source extraction service. You can retain the default value.
	logging.config: Set the log path of the verification service to the absolute path of the config/log4j2source.xml file.
	spring.check.server-uri: The request address of the verification service. You can retain the default value.
	spring.extract.schema: The schema of the data to be verified (i.e., the MySQL database name).
	spring.extract.core-pool-size: The number of concurrent threads. You can retain the default value or change it based on the site requirements.
	bootstrap-servers: The Kafka working address. You can retain the default value.
	
	Configure data sources.
	By default, the tool uses the Druid data source. You can customize connection pool parameters based on the number of database verification tasks (number of tables).
	driver-class-name: The database driver name. You can set this parameter based on the source database type. For details, see the configuration file template.
    url: The JDBC connection string. You can set this parameter based on the source database type and database name. For details, see the configuration file template.
    username: The source database username.
    password: The source database password, which must be enclosed in single quotation marks (').
    initialSize: The number of connections in the JDBC connection pool. You can retain the default value.
    minIdle: The default minimum number of connections in the connection pool.
    maxActive: The default number of activated database connections.
    validationQuery: The JDBC keepalive query statement. Do not modify this parameter.
	
```

**Configuring Service Startup on the Destination End**

```
To configure the destination service, modify the `application-sink.yml` file.
	server.port: The web port of the destination extraction service. You can retain the default value.
	logging.config: Set the log path of the verification service to the absolute path of the config/log4j2sink.xml file.
	spring.check.server-uri: The request address of the verification service. You can retain the default value.
	spring.extract.schema: The schema of the data to be verified (i .e., the openGauss schema name).
	spring.extract.core-pool-size: The number of concurrent threads. You can retain the default value or change it based on the site requirements.
	bootstrap-servers: The Kafka working address. You can retain the default value.
	
	Configure data sources.
	By default, the tool uses the Druid data source. You can customize connection pool parameters based on the number of database verification tasks (number of tables).
	driver-class-name: The database driver name. Set this parameter based on the type of the destination database. For details, see the configuration file template.
    url: The JDBC connection string. You can set this parameter based on the destination database type and database name. For details, see the configuration file template.
    username: The destination database username.
    password: The destination database password, which must be enclosed in single quotation marks (').
    initialSize: The number of connections in the JDBC connection pool. You can retain the default value.
    minIdle: The default minimum number of connections in the connection pool.
    maxActive: The default number of activated database connections.
```



**Starting the Data Verification Service**

```shell
sh extract-endpoints.sh start|restart|stop
sh check-endpoint.sh start|restart|stop
Start the extraction service and then the verification service.
```

**Starting the Background**

```shell
nohup java -jar datachecker-extract-0.0.1.jar --source  >/dev/null 2>&1 &

nohup java -jar datachecker-extract-0.0.1.jar --sink >/dev/null 2>&1 &

nohup java -jar datachecker-check-0.0.1.jar >/dev/null 2>&1 &
```

The verification request initiates automatically after the service starts.

**Remarks**

```
1. To verify a single instance, use the `sh` script. For the concurrent verification, clone the current working directory file, reconfigure it, and use the Java background startup command.
2. After the extraction service is started, it automatically loads the table information from the database. If the data volume is large, the data loading process may take a long time.
3. The verification service checks if extraction is complete. If no, the verification service automatically exits. In this case, check logs to see the table information loading progress on the source and destination ends. Alternatively, restart the verification service.
4. To start the incremental verification service, modify `debezium-enable:true` in the source configuration file `\config\application-source.yml` and configure other Debezium-related settings.
```

**Starting the Service Locally**

Add the VM parameter `VM Option` to the startup configuration:

```
Source extraction service
-Dspring.config.additional-location=.\config\application-source.yml

Sink extraction service
-Dspring.config.additional-location=.\config\application-sink.yml

Verification service
-Dspring.config.additional-location=.\config\application.yml
```

**Constraints**

```
JDK 11 or later is required.
The current version only supports data verification between MySQL and openGauss databases.
The current version supports only data verification and does not support table object verification.
MySQL 5.7 or later is required.
Geography/geometry types support openGauss-to-openGauss only. Bit types do not support openGauss-to-MySQL verification.
```



#### Contributions

1. Fork this repository.
2. Create a Feat_xxx branch.
3. Commit the code.
4. Create a pull request.


#### Download Addresses

[Download](https://opengauss.obs.cn-south-1.myhuaweicloud.com/latest/tools/gs_datacheck-5.0.0.tar.gz)
