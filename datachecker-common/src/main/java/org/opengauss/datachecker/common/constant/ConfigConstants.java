/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.common.constant;

/**
 * ConfigConstants
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/31
 * @since ：11
 */
public interface ConfigConstants {
    /**
     * csv config : spring.csv.schema
     */
    String CSV_SCHEMA = "spring.csv.schema";

    /**
     * csv config : spring.csv.path
     */
    String CSV_PATH = "spring.csv.path";

    /**
     * csv config : spring.csv.sync
     */
    String CSV_SYNC = "spring.csv.sync";

    /**
     * csv config : spring.csv.path:data
     */
    String CSV_DATA_PATH = "spring.csv.path:data";

    /**
     * csv config : spring.csv.path:reader
     */
    String CSV_READER_PATH = "spring.csv.path:reader";

    /**
     * csv config : spring.csv.path:writer
     */
    String CSV_WRITER_PATH = "spring.csv.path:writer";

    /**
     * csv config : spring.csv.schema-tables
     */
    String CSV_SCHEMA_TABLES_PATH = "spring.csv.schema-tables";

    /**
     * csv config : spring.csv.schema-columns
     */
    String CSV_SCHEMA_COLUMNS_PATH = "spring.csv.schema-columns";

    /**
     * csv config : spring.csv.sleep-interval
     */
    String CSV_SLEEP_INTERVAL = "spring.csv.sleep-interval";

    /**
     * csv config : spring.csv.task-dispatcher-interval
     */
    String CSV_TASK_DISPATCHER_INTERVAL = "spring.csv.task-dispatcher-interval";

    /**
     * csv config : spring.csv.max-dispatcher-size
     */
    String CSV_MAX_DISPATCHER_SIZE = "spring.csv.max-dispatcher-size";

    /**
     * check_mode
     */
    String CHECK_MODE = "check_mode";

    /**
     * check.process.id
     */
    String PROCESS_NO = "check.process.id";

    /**
     * check.start-time
     */
    String START_LOCAL_TIME = "check.start-time";

    /**
     * jdbc.result-set.fetch-size
     */
    String FETCH_SIZE = "jdbc.result-set.fetch-size";

    /**
     * spring.extract.endpoint
     */
    String ENDPOINT = "spring.extract.endpoint";

    /**
     * spring.extract.query-dop
     */
    String QUERY_DOP = "spring.extract.query-dop";

    /**
     * spring.extract.databaseType
     */
    String DATA_BASE_TYPE = "spring.extract.databaseType";

    /**
     * spring.extract.object-size-expansion-factor
     */
    String OBJECT_SIZE_EXPANSION_FACTOR = "spring.extract.object-size-expansion-factor";

    /**
     * spring.memory-monitor-enable
     */
    String MEMORY_MONITOR = "spring.memory-monitor-enable";

    /**
     * spring.check.maximum-table-slice-size
     */
    String MAXIMUM_TABLE_SLICE_SIZE = "spring.check.maximum-table-slice-size";

    /**
     * spring.check.maximum-topic-size
     */
    String MAXIMUM_TOPIC_SIZE = "spring.check.maximum-topic-size";

    /**
     * spring.check.floating-point-data-supply-zero
     */
    String FLOATING_POINT_DATA_SUPPLY_ZERO = "spring.check.floating-point-data-supply-zero";

    /**
     * with table slice check, config the maximum number of threads in the thread pool
     */
    String EXTEND_MAXIMUM_POOL_SIZE = "spring.check.extend-maximum-pool-size";

    /**
     * with table slice check, config the maximum number of threads in the thread pool
     */
    String MAXIMUM_POOL_SIZE = "spring.check.maximum-pool-size";

    /**
     * data.check.bucket-expect-capacity
     */
    String BUCKET_CAPACITY = "data.check.bucket-expect-capacity";

    /**
     * data.check.data-path
     */
    String CHECK_PATH = "data.check.data-path";

    /**
     * data.check.sql_mode_pad_char_to_full_length
     */
    String SQL_MODE_PAD_CHAR_TO_FULL_LENGTH = "data.check.sql_mode_pad_char_to_full_length";

    /**
     * pad_char_to_full_length
     */
    String SQL_MODE_NAME_PAD_CHAR_TO_FULL_LENGTH = "pad_char_to_full_length";

    /**
     * data.check.sql_mode_value_cache
     */
    String SQL_MODE_VALUE_CACHE = "data.check.sql_mode_value_cache";

    /**
     * data.check.sql_mode_force_refresh
     */
    String SQL_MODE_FORCE_REFRESH = "data.check.sql_mode_force_refresh";

    /**
     * spring.check.heart-beat-heath
     */
    String ENABLE_HEART_BEAT_HEATH = "spring.check.heart-beat-heath";

    /**
     * spring.kafka.bootstrap-servers
     */
    String KAFKA_SERVERS = "spring.kafka.bootstrap-servers";

    /**
     * spring.kafka.consumer.enable-auto-commit
     */
    String KAFKA_AUTO_COMMIT = "spring.kafka.consumer.enable-auto-commit";

    /**
     * spring.kafka.consumer.group-id
     */
    String KAFKA_DEFAULT_GROUP_ID = "spring.kafka.consumer.group-id";

    /**
     * spring.kafka.consumer.auto-offset-reset
     */
    String KAFKA_AUTO_OFFSET_RESET = "spring.kafka.consumer.auto-offset-reset";

    /**
     * spring.kafka.consumer.request-timeout-ms
     */
    String KAFKA_REQUEST_TIMEOUT = "spring.kafka.consumer.request-timeout-ms";

    /**
     * spring.kafka.consumer.fetch-max-bytes
     */
    String KAFKA_FETCH_MAX_BYTES = "spring.kafka.consumer.fetch-max-bytes";

    /**
     * spring.kafka.consumer.max-poll-records
     */
    String KAFKA_MAX_POLL_RECORDS = "spring.kafka.consumer.max-poll-records";

    /**
     * spring.datasource.driver-class-name
     */
    String DRIVER_CLASS_NAME = "spring.datasource.driver-class-name";

    /**
     * spring.datasource.url
     */
    String DS_URL = "spring.datasource.url";

    /**
     * spring.datasource.username
     */
    String DS_USER_NAME = "spring.datasource.username";

    /**
     * spring.datasource.password
     */
    String DS_PASSWORD = "spring.datasource.password";

    /**
     * spring.datasource.druid.initialSize
     */
    String DRUID_INITIAL_SIZE = "spring.datasource.druid.initialSize";

    /**
     * spring.datasource.druid.minIdle
     */
    String DRUID_MIN_IDLE = "spring.datasource.druid.minIdle";

    /**
     * spring.datasource.druid.maxActive
     */
    String DRUID_MAX_ACTIVE = "spring.datasource.druid.maxActive";

    /**
     * spring.datasource.druid.max-wait
     */
    String DRUID_MAX_WAIT = "spring.datasource.druid.max-wait";

    /**
     * spring.datasource.druid.validationQuery
     */
    String DRUID_VALIDATION_QUERY = "spring.datasource.druid.validationQuery";

    /**
     * spring.datasource.druid.min-evictable-idle-time-millis
     */
    String DRUID_MIN_EVICTABLE_IDLE_TIME_MILLIS = "spring.datasource.druid.min-evictable-idle-time-millis";

    /**
     * lifecycle graceful shutdown wait time
     */
    String TIMEOUT_PER_SHUTDOWN_PHASE = "spring.lifecycle.timeout-per-shutdown-phase";

    /**
     * openGauss.Compatibility.B
     */
    String OG_COMPATIBILITY_B = "openGauss.Compatibility.B";

    /**
     * data check sink database info
     */
    String DATA_CHECK_SINK_DATABASE = "data.check.sink.database";

    /**
     * data check source database info
     */
    String DATA_CHECK_SOURCE_DATABASE = "data.check.source.database";

    /**
     * spring.extract.debezium-row-display
     */
    String DEBEZIUM_ROW_DISPLAY = "spring.extract.debezium-row-display";

    /**
     * spring.check.max-retry-times
     */
    String MAX_RETRY_TIMES = "spring.check.max-retry-times";

    /**
     * spring.check.rest-api-page-size
     */
    String REST_API_PAGE_SIZE = "spring.check.rest-api-page-size";

    /**
     * data.check.auto-delete-topic
     */
    String AUTO_DELETE_TOPIC = "data.check.auto-delete-topic";

    /**
     * lower_case_table_names
     */
    String LOWER_CASE_TABLE_NAMES = "lower_case_table_names";
}
