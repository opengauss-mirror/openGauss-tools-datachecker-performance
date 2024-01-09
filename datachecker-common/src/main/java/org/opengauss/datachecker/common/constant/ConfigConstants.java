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
    String CSV_SCHEMA = "spring.csv.schema";
    String CSV_PATH = "spring.csv.path";
    String CSV_SYNC = "spring.csv.sync";
    String CSV_DATA_PATH = "spring.csv.path:data";
    String CSV_READER_PATH = "spring.csv.path:reader";
    String CSV_WRITER_PATH = "spring.csv.path:writer";
    String CSV_SCHEMA_TABLES_PATH = "spring.csv.schema-tables";
    String CSV_SCHEMA_COLUMNS_PATH = "spring.csv.schema-columns";
    String CSV_SLEEP_INTERVAL = "spring.csv.sleep-interval";
    String CSV_TASK_DISPATCHER_INTERVAL = "spring.csv.task-dispatcher-interval";
    String CSV_MAX_DISPATCHER_SIZE = "spring.csv.max-dispatcher-size";

    String CHECK_MODE = "check_mode";
    String PROCESS_NO = "check.process.id";
    String START_LOCAL_TIME = "check.start-time";
    String FETCH_SIZE = "jdbc.result-set.fetch-size";

    String ENDPOINT = "spring.extract.endpoint";
    String QUERY_DOP = "spring.extract.query-dop";
    String DATA_BASE_TYPE = "spring.extract.databaseType";
    String OBJECT_SIZE_EXPANSION_FACTOR = "spring.extract.object-size-expansion-factor";


    String MEMORY_MONITOR = "spring.memory-monitor-enable";

    String MAXIMUM_TABLE_SLICE_SIZE = "spring.check.maximum-table-slice-size";
    String MAXIMUM_TOPIC_SIZE = "spring.check.maximum-topic-size";
    String FLOATING_POINT_DATA_SUPPLY_ZERO = "spring.check.floating-point-data-supply-zero";

    /**
     * with table slice check, config the maximum number of threads in the thread pool
     */
    String EXTEND_MAXIMUM_POOL_SIZE = "spring.check.extend-maximum-pool-size";
    String BUCKET_CAPACITY = "data.check.bucket-expect-capacity";
    String CHECK_PATH = "data.check.data-path";

    String KAFKA_SERVERS = "spring.kafka.bootstrap-servers";
    String KAFKA_AUTO_COMMIT = "spring.kafka.consumer.enable-auto-commit";
    String KAFKA_DEFAULT_GROUP_ID = "spring.kafka.consumer.group-id";
    String KAFKA_AUTO_OFFSET_RESET = "spring.kafka.consumer.auto-offset-reset";
    String KAFKA_REQUEST_TIMEOUT = "spring.kafka.consumer.request-timeout-ms";
    String KAFKA_FETCH_MAX_BYTES = "spring.kafka.consumer.fetch-max-bytes";
    String KAFKA_MAX_POLL_RECORDS = "spring.kafka.consumer.max-poll-records";

    String DRUID_INITIAL_SIZE = "spring.datasource.druid.initialSize";
    String DRUID_MIN_IDLE = "spring.datasource.druid.minIdle";
    String DRUID_MAX_ACTIVE = "spring.datasource.druid.maxActive";
    String DRUID_MAX_WAIT = "spring.datasource.druid.max-wait";
    String DRUID_VALIDATION_QUERY = "spring.datasource.druid.validationQuery";
    String DRUID_MIN_EVICTABLE_IDLE_TIME_MILLIS = "spring.datasource.druid.min-evictable-idle-time-millis";

    /**
     * lifecycle graceful shutdown wait time
     */
    String TIMEOUT_PER_SHUTDOWN_PHASE = "spring.lifecycle.timeout-per-shutdown-phase";

}
