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

package org.opengauss.datachecker.extract.debezium;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.check.IncrementCheckConfig;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.exception.DebeziumConfigException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.config.KafkaConsumerConfig;
import org.opengauss.datachecker.extract.kafka.KafkaAdminService;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.PreDestroy;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 * DataConsolidationServiceImpl
 *
 * @author ：zhangyaozhong
 * @date ：Created in 2022/6/14
 * @since ：11
 */
@Service
public class DataConsolidationServiceImpl implements DataConsolidationService {
    private static final IncrementCheckConfig INCREMENT_CHECK_CONIFG = new IncrementCheckConfig();
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final String MYSQL_DATE_TYPE = "date";
    private static final Logger LOG = LogUtils.getLogger();

    @Autowired
    private DebeziumConsumerListener debeziumListener;
    @Autowired
    private KafkaAdminService kafkaAdminService;
    @Autowired
    private KafkaConsumerConfig kafkaConfig;
    @Autowired
    private ExtractProperties extractProperties;
    @Autowired
    private MetaDataService metaDataService;
    private DebeziumWorker worker = null;
    private ExecutorService executorService = null;

    /**
     * initIncrementConfig
     */
    @Override
    public void initIncrementConfig() {
        if (extractProperties.isDebeziumEnable()) {
            worker = new DebeziumWorker(debeziumListener, kafkaConfig);
            executorService = ThreadUtil.newSingleThreadExecutor();
            executorService.submit(worker);
        }
    }

    @PreDestroy
    public void close() {
        if (worker != null) {
            worker.close();
        }
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    @Override
    public void pauseOrResumeIncrementMonitor(Boolean pauseOrResume) {
        if (Objects.nonNull(worker)) {
            worker.switchPauseOrResume(pauseOrResume);
        }
    }

    /**
     * Get the topic records of debezium, and analyze and merge the topic records
     *
     * @return topic records
     */
    @Override
    public List<SourceDataLog> getDebeziumTopicRecords(int fetchOffset) {
        checkIncrementCheckEnvironment();
        int begin = 0;
        DebeziumDataLogs debeziumDataLogs = new DebeziumDataLogs(metaDataService);
        while (begin <= fetchOffset) {
            DebeziumDataBean debeziumDataBean = debeziumListener.poll();
            if (Objects.isNull(debeziumDataBean)) {
                break;
            }
            mysqlDateConvert(debeziumDataBean);
            debeziumDataLogs.addDebeziumDataKey(debeziumDataBean);
            begin++;
        }
        return new ArrayList<>(debeziumDataLogs.values());
    }

    private void mysqlDateConvert(DebeziumDataBean debeziumDataBean) {
        final TableMetadata tableMetadata = metaDataService.getMetaDataOfSchemaByCache(debeziumDataBean.getTable());
        if (Objects.isNull(tableMetadata)) {
            return;
        }
        final List<ColumnsMetaData> columnsMetas = tableMetadata.getColumnsMetas();
        final List<String> dateList = columnsMetas.stream()
                                                  .filter(column -> StringUtils.equalsIgnoreCase(column.getColumnType(),
                                                      MYSQL_DATE_TYPE))
                                                  .map(ColumnsMetaData::getColumnName)
                                                  .collect(Collectors.toList());
        final Map<String, String> valueMap = debeziumDataBean.getData();
        dateList.forEach(dateField -> {
            final String time = valueMap.get(dateField);
            if (Objects.isNull(time)) {
                valueMap.put(dateField, null);
            } else {
                valueMap.put(dateField, decompressLocalDate(Integer.parseInt(time)));
            }
        });
    }

    private String decompressLocalDate(int compressDate) {
        return LocalDate.ofEpochDay(compressDate)
                        .format(DATE_FORMATTER);
    }

    /**
     * Get the debezium listening table and record the offset information of the message corresponding to the topic
     *
     * @return offset
     */
    @Override
    public int getDebeziumTopicRecordEndOffSet() {
        // View topic current message consumption deadline
        return debeziumListener.size();
    }

    @Override
    public boolean isSourceEndpoint() {
        return Objects.equals(Endpoint.SOURCE, extractProperties.getEndpoint());
    }

    /**
     * Is the current service a source service
     */
    private void checkSourceEndpoint() {
        Assert.isTrue(isSourceEndpoint(), "The current service is not a source-endpoint-service");
    }

    /**
     * Check the configuration of the debezium environment for incremental verification
     */
    private void checkIncrementCheckEnvironment() {
        final Set<String> allKeys = metaDataService.queryMetaDataOfSchemaCache()
                                                   .keySet();
        checkDebeziumEnvironment(extractProperties.getDebeziumTopic(), extractProperties.getDebeziumTables(), allKeys);
    }

    /**
     * Check and configure the incremental verification debezium environment configuration
     *
     * @param config configuration information
     */
    @Override
    public void configIncrementCheckEnvironment(@NotNull IncrementCheckConfig config) {
        final Set<String> allKeys = MetaDataCache.getAllKeys();
        checkDebeziumEnvironment(config.getDebeziumTopic(), config.getDebeziumTables(), allKeys);
        INCREMENT_CHECK_CONIFG.setDebeziumTables(config.getDebeziumTables())
                              .setDebeziumTopic(config.getDebeziumTopic())
                              .setGroupId(config.getGroupId())
                              .setPartitions(config.getPartitions());
    }

    /**
     * debezium Environment check<p>
     * Check whether the debezium configuration topic exists<p>
     * Check whether the debezium configuration tables exist<p>
     *
     * @param debeziumTopic  Topic to be checked
     * @param debeziumTables Debezium configuration table list
     * @param allTableSet    Source end table set
     */
    private void checkDebeziumEnvironment(
        @NotEmpty(message = "Debezium configuration topic cannot be empty") String debeziumTopic,
        List<String> debeziumTables,
        @NotEmpty(message = "Source side table metadata cache exception") Set<String> allTableSet) {
        checkSourceEndpoint();
        if (!kafkaAdminService.isTopicExists(debeziumTopic)) {
            // The configuration item debezium topic information does not exist
            throw new DebeziumConfigException(
                "The configuration item debezium topic " + debeziumTopic + " information does not exist");
        }

        if (CollectionUtils.isEmpty(debeziumTables)) {
            LOG.warn("in current period , debezium table is empty");
            return;
        }
        final List<String> allTableList = allTableSet.stream()
                                                     .map(String::toUpperCase)
                                                     .collect(Collectors.toList());
        List<String> invalidTables = debeziumTables.stream()
                                                   .map(String::toUpperCase)
                                                   .filter(table -> !allTableList.contains(table))
                                                   .collect(Collectors.toList());

        if (!CollectionUtils.isEmpty(invalidTables)) {
            // The configuration item debezium tables contains non-existent or black and white list tables
            LOG.warn("in current period , debezium has no table to check");
            throw new DebeziumConfigException(
                "The configuration item debezium-tables contains non-existent or black-and-white list tables:"
                    + invalidTables.toString());
        }
    }
}
