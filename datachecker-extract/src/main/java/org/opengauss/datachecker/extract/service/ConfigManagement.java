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

package org.opengauss.datachecker.extract.service;

import com.alibaba.druid.pool.DruidDataSource;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.csv.CsvPathConfig;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.util.SpringUtil;
import org.opengauss.datachecker.extract.config.DataSourceConfig;
import org.opengauss.datachecker.extract.config.DruidDataSourceConfig;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.kafka.KafkaAdminService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Objects;

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
    private ExtractProperties properties;
    @Resource
    private KafkaAdminService kafkaAdminService;

    @Value("${spring.check.maximum-topic-size}")
    private int maximumTopicSize = 3;
    @Value("${spring.memory-monitor-enable}")
    private boolean isEnableMemoryMonitor;
    @Value("${spring.kafka.bootstrap-servers}")
    private String servers;
    @Value("${spring.extract.query-dop}")
    private int queryDop;
    @Value("${spring.check.maximum-table-slice-size}")
    private int maximumTableSliceSize;
    @Value("${spring.check.extend-maximum-pool-size}")
    private int extendMaxPoolSize = 10;

    @Value("${spring.datasource.druid.initialSize}")
    private int initialSize;
    @Value("${spring.datasource.druid.minIdle}")
    private int minIdle;
    @Value("${spring.datasource.druid.maxActive}")
    private int maxActive;
    @Value("${spring.datasource.druid.max-wait}")
    private int maxWait;
    @Value("${spring.datasource.druid.validationQuery}")
    private String validationQuery;
    @Value("${spring.datasource.druid.min-evictable-idle-time-millis}")
    private int minEvictableIdleTimeMillis;
    @Value("${spring.lifecycle.timeout-per-shutdown-phase}")
    private int timeoutPerShutdownPhase;
    @Value("${spring.extract.object-size-expansion-factor}")
    private int objectSizeExpansionFactor;

    /**
     * init csv config
     *
     * @param csvPathConfig csvPathConfig
     */
    public void initCsvConfig(CsvPathConfig csvPathConfig) {
        setCsvConfig(csvPathConfig);
    }

    private static void setCsvConfig(CsvPathConfig config) {
        ConfigCache.put(ConfigConstants.CSV_SYNC, config.isSync());
        ConfigCache.put(ConfigConstants.CSV_SCHEMA, config.getSchema());
        ConfigCache.put(ConfigConstants.CSV_PATH, config.getPath());
        ConfigCache.put(ConfigConstants.CSV_DATA_PATH, config.getData());
        ConfigCache.put(ConfigConstants.CSV_READER_PATH, config.getReader());
        ConfigCache.put(ConfigConstants.CSV_WRITER_PATH, config.getWriter());
        ConfigCache.put(ConfigConstants.CSV_SLEEP_INTERVAL, config.getSleepInterval());
        ConfigCache.put(ConfigConstants.CSV_SCHEMA_TABLES_PATH, config.getSchemaTables());
        ConfigCache.put(ConfigConstants.CSV_SCHEMA_COLUMNS_PATH, config.getSchemaColumns());
        ConfigCache.put(ConfigConstants.CHECK_MODE, CheckMode.CSV);
    }

    /**
     * load extract properties
     */
    public void loadExtractProperties() {
        DataSourceConfig config = SpringUtil.getBean(DataSourceConfig.class);
        if (config instanceof DruidDataSourceConfig) {
            setDataSourceConfig((DruidDataSource) ((DruidDataSourceConfig) config).druidDataSource());
        } else {
            setDataSourceConfig(null);
        }

        setExtractConfig(properties);
        ConfigCache.put(ConfigConstants.MAXIMUM_TOPIC_SIZE, maximumTopicSize);
        ConfigCache.put(ConfigConstants.MEMORY_MONITOR, isEnableMemoryMonitor);
        ConfigCache.put(ConfigConstants.QUERY_DOP, queryDop);
        ConfigCache.put(ConfigConstants.MAXIMUM_TABLE_SLICE_SIZE, maximumTableSliceSize);
        ConfigCache.put(ConfigConstants.FETCH_SIZE, 1000);
        ConfigCache.put(ConfigConstants.TIMEOUT_PER_SHUTDOWN_PHASE, timeoutPerShutdownPhase);
        ConfigCache.put(ConfigConstants.EXTEND_MAXIMUM_POOL_SIZE, extendMaxPoolSize);

        loadKafkaProperties();
    }

    public void loadKafkaProperties() {
        ConfigCache.put(ConfigConstants.KAFKA_SERVERS, servers);
        kafkaAdminService.initAdminClient();
    }

    private void setDataSourceConfig(DruidDataSource bean) {
        if (Objects.isNull(bean)) {
            ConfigCache.put(ConfigConstants.DRUID_MAX_ACTIVE, 100);
            return;
        }
        ConfigCache.put(ConfigConstants.DRUID_INITIAL_SIZE, initialSize);
        ConfigCache.put(ConfigConstants.DRUID_MIN_IDLE, minIdle);
        ConfigCache.put(ConfigConstants.DRUID_MAX_ACTIVE, maxActive);
        ConfigCache.put(ConfigConstants.DRUID_MAX_WAIT, maxWait);
        ConfigCache.put(ConfigConstants.DRUID_VALIDATION_QUERY, validationQuery);
        ConfigCache.put(ConfigConstants.DRUID_MIN_EVICTABLE_IDLE_TIME_MILLIS, minEvictableIdleTimeMillis);
    }

    private void setExtractConfig(ExtractProperties properties) {
        ConfigCache.put(ConfigConstants.ENDPOINT, properties.getEndpoint());
        ConfigCache.put(ConfigConstants.DATA_BASE_TYPE, properties.getDatabaseType());
        ConfigCache.put(ConfigConstants.OBJECT_SIZE_EXPANSION_FACTOR, objectSizeExpansionFactor);
    }
}
