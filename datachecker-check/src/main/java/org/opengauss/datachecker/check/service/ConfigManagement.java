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

package org.opengauss.datachecker.check.service;

import org.opengauss.datachecker.check.config.DataCheckProperties;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.csv.CsvPathConfig;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.util.IdGenerator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * ConfigManagement
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/31
 * @since ：11
 */
@Component
public class ConfigManagement {
    @Resource
    private DataCheckProperties checkProperties;
    @Value("${spring.kafka.bootstrap-servers}")
    private String servers;
    @Value("${spring.kafka.consumer.enable-auto-commit}")
    private boolean isEnableAutoCommit;
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;
    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;
    @Value("${spring.kafka.consumer.max-poll-records}")
    private int maxPollRecordsConfig;
    @Value("${spring.kafka.consumer.fetch-max-bytes}")
    private int fetchMaxBytes;
    @Value("${spring.kafka.consumer.request-timeout-ms}")
    private int requestTimeoutMs;
    @Value("${spring.check.floating-point-data-supply-zero}")
    private boolean floatingPointDataSupplyZero;
    @Value("${spring.check.heart-beat-heath}")
    private boolean enableHeartBeatHeath;
    @Value("${spring.check.maximum-topic-size}")
    private int maxTopicSize;
    @Value("${spring.check.rest-api-page-size}")
    private int restApiPageSize;
    @Value("${data.check.auto-delete-topic}")
    private int autoDeleteTopic;
    @Value("${data.check.sql_mode_pad_char_to_full_length}")
    private boolean sqlModePadCharToFullLength;
    @Value("${data.check.create-repair-sql}")
    private boolean isCreateRepairSql;
    @Value("${spring.check.maximum-pool-size}")
    private int maxPoolSize = 10;
    /**
     * config management init
     */
    public void init() {
        ConfigCache.put(ConfigConstants.PROCESS_NO, IdGenerator.nextId36());
        ConfigCache.put(ConfigConstants.MAXIMUM_TOPIC_SIZE, maxTopicSize);
        ConfigCache.put(ConfigConstants.CHECK_PATH, checkProperties.getDataPath());
        ConfigCache.put(ConfigConstants.BUCKET_CAPACITY, checkProperties.getBucketExpectCapacity());
        ConfigCache.put(ConfigConstants.ENDPOINT, Endpoint.CHECK);
        ConfigCache.put(ConfigConstants.FLOATING_POINT_DATA_SUPPLY_ZERO, floatingPointDataSupplyZero);
        ConfigCache.put(ConfigConstants.SQL_MODE_PAD_CHAR_TO_FULL_LENGTH, sqlModePadCharToFullLength);
        ConfigCache.put(ConfigConstants.ENABLE_HEART_BEAT_HEATH, enableHeartBeatHeath);
        ConfigCache.put(ConfigConstants.REST_API_PAGE_SIZE, restApiPageSize);
        ConfigCache.put(ConfigConstants.AUTO_DELETE_TOPIC, autoDeleteTopic);
        ConfigCache.put(ConfigConstants.MAXIMUM_POOL_SIZE, maxPoolSize);
        ConfigCache.put(ConfigConstants.CREATE_REPAIR_SQL, isCreateRepairSql);
        initKafka();
    }

    private void initKafka() {
        ConfigCache.put(ConfigConstants.KAFKA_SERVERS, servers);
        ConfigCache.put(ConfigConstants.KAFKA_AUTO_COMMIT, isEnableAutoCommit);
        ConfigCache.put(ConfigConstants.KAFKA_DEFAULT_GROUP_ID, groupId);
        ConfigCache.put(ConfigConstants.KAFKA_AUTO_OFFSET_RESET, autoOffsetReset);
        ConfigCache.put(ConfigConstants.KAFKA_MAX_POLL_RECORDS, maxPollRecordsConfig);
        ConfigCache.put(ConfigConstants.KAFKA_FETCH_MAX_BYTES, fetchMaxBytes);
        ConfigCache.put(ConfigConstants.KAFKA_REQUEST_TIMEOUT, requestTimeoutMs);
    }

    public void setCsvConfig(CsvPathConfig config) {
        ConfigCache.put(ConfigConstants.CSV_SYNC, config.isSync());
        ConfigCache.put(ConfigConstants.CSV_SCHEMA, config.getSchema());
        ConfigCache.put(ConfigConstants.CSV_PATH, config.getPath());
        ConfigCache.put(ConfigConstants.CSV_DATA_PATH, config.getData());
        ConfigCache.put(ConfigConstants.CSV_READER_PATH, config.getReader());
        ConfigCache.put(ConfigConstants.CSV_WRITER_PATH, config.getWriter());
        ConfigCache.put(ConfigConstants.CSV_SLEEP_INTERVAL, config.getSleepInterval());
        ConfigCache.put(ConfigConstants.CSV_SCHEMA_TABLES_PATH, config.getSchemaTables());
        ConfigCache.put(ConfigConstants.CSV_SCHEMA_COLUMNS_PATH, config.getSchemaColumns());
        ConfigCache.put(ConfigConstants.CSV_TASK_DISPATCHER_INTERVAL, config.getTaskDispatcherInterval());
        ConfigCache.put(ConfigConstants.CSV_MAX_DISPATCHER_SIZE, config.getMaxDispatcherSize());
        ConfigCache.put(ConfigConstants.CHECK_MODE, CheckMode.CSV);
    }
}
