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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.constant.Constants;
import org.opengauss.datachecker.common.entry.enums.DML;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.Database;
import org.opengauss.datachecker.common.entry.extract.ExtractConfig;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.TableMetadataHash;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.exception.BuildRepairStatementException;
import org.opengauss.datachecker.common.exception.ProcessMultipleException;
import org.opengauss.datachecker.common.exception.TableNotExistException;
import org.opengauss.datachecker.common.exception.TaskNotFoundException;
import org.opengauss.datachecker.common.service.DynamicThreadPoolManager;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.common.util.TopicUtil;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.cache.TableExtractStatusCache;
import org.opengauss.datachecker.extract.cache.TopicCache;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.kafka.KafkaAdminService;
import org.opengauss.datachecker.extract.task.DataManipulationService;
import org.opengauss.datachecker.extract.task.ExtractTaskBuilder;
import org.opengauss.datachecker.extract.task.ExtractTaskRunnable;
import org.opengauss.datachecker.extract.task.ExtractThreadSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.opengauss.datachecker.common.constant.DynamicTpConstant.EXTRACT_EXECUTOR;

/**
 * DataExtractServiceImpl
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/1
 * @since ：11
 */
@Slf4j
@Service
public class DataExtractServiceImpl implements DataExtractService {
    private static final Logger logKafka = LogUtils.geKafkaLogger();
    /**
     * Maximum number of sleeps of threads executing data extraction tasks
     */
    private static final int MAX_SLEEP_COUNT = 5;
    /**
     * The sleep time of the thread executing the data extraction task each time, in milliseconds
     */
    private static final int MAX_SLEEP_MILLIS_TIME = 2000;
    private static final int MAX_QUERY_PAGE_SIZE = 500;
    private static final String PROCESS_NO_RESET = "0";

    /**
     * After the service is started, the {code atomicProcessNo} attribute will be initialized,
     * <p>
     * When the user starts the verification process, the {code atomicProcessNo} attribute will be verified and set
     */
    private final AtomicReference<String> atomicProcessNo = new AtomicReference<>(PROCESS_NO_RESET);

    private final AtomicReference<List<ExtractTask>> taskReference = new AtomicReference<>();
    @Autowired
    private ExtractTaskBuilder extractTaskBuilder;
    @Autowired
    private ExtractThreadSupport extractThreadSupport;
    @Autowired
    private CheckingFeignClient checkingFeignClient;
    @Autowired
    private ExtractProperties extractProperties;
    @Autowired
    private MetaDataService metaDataService;
    @Autowired
    private DataManipulationService dataManipulationService;
    @Resource
    private TopicCache topicCache;
    @Value("${server.port}")
    private int serverPort = 0;
    @Value("${spring.check.maximum-topic-size}")
    private int maximumTopicSize = 10;
    @Resource
    private DynamicThreadPoolManager dynamicThreadPoolManager;
    @Resource
    private KafkaAdminService kafkaAdminService;

    /**
     * Data extraction service
     * <p>
     * The verification service verifies the process number by issuing a request for data extraction process,
     * so as to prevent repeated starting commands at the same time
     * <p>
     * According to the metadata cache information, build a data extraction task,
     * save the current task information to {@code taskReference},
     * and wait for the verification service to initiate the task execution instruction.
     * <p>
     * Submit the task list to the verification service.
     *
     * @param processNo Execution process number
     * @throws ProcessMultipleException The previous instance is executing the data extraction service.
     *                                  It cannot restart the new verification
     *                                  and throws a ProcessMultipleException exception.
     */
    @Override
    public List<ExtractTask> buildExtractTaskAllTables(String processNo) throws ProcessMultipleException {
        // If the calling end point is not the source end, it directly returns null
        if (!Objects.equals(extractProperties.getEndpoint(), Endpoint.SOURCE)) {
            log.info("The current endpoint is not the source endpoint, and the task cannot be built");
            return new ArrayList<>(0);
        }
        topicCache.initEndpoint(extractProperties.getEndpoint());
        if (atomicProcessNo.compareAndSet(PROCESS_NO_RESET, processNo)) {
            Set<String> tableNames = MetaDataCache.getAllKeys();
            List<ExtractTask> taskList = extractTaskBuilder.builder(tableNames);
            if (CollectionUtils.isEmpty(taskList)) {
                return taskList;
            }
            taskReference.set(taskList);
            log.info("build extract task process={} count={}", processNo, taskList.size());
            atomicProcessNo.set(processNo);
            return taskList;
        } else {
            log.error("process={} is running extract task , {} please wait ... ", atomicProcessNo.get(), processNo);
            throw new ProcessMultipleException("process {" + atomicProcessNo.get() + "} is running extract task");
        }
    }

    /**
     * Destination task configuration
     *
     * @param processNo Execution process number
     * @param taskList  taskList
     * @throws ProcessMultipleException The previous instance is executing the data extraction service.
     *                                  It cannot restart the new verification
     *                                  and throws a ProcessMultipleException exception.
     */
    @Override
    public void buildExtractTaskAllTables(String processNo, @NonNull List<ExtractTask> taskList)
        throws ProcessMultipleException {
        if (!Objects.equals(extractProperties.getEndpoint(), Endpoint.SINK)) {
            return;
        }
        if (CollectionUtils.isEmpty(taskList)) {
            return;
        }
        // Verify whether the task list built on the source side exists on the destination side,
        // and filter the nonexistent task list
        final Set<String> tableNames = MetaDataCache.getAllKeys();
        if (atomicProcessNo.compareAndSet(PROCESS_NO_RESET, processNo)) {
            if (CollectionUtils.isEmpty(taskList) || CollectionUtils.isEmpty(tableNames)) {
                log.info("build extract task process={} taskList={} ,MetaCache tableNames={}", processNo,
                    taskList.size(), tableNames);
                return;
            }
            final List<ExtractTask> extractTasks =
                taskList.stream().filter(task -> tableNames.contains(task.getTableName())).collect(Collectors.toList());
            extractTasks.forEach(this::updateSinkMetadata);
            taskReference.set(extractTasks);
            log.info("build extract task process={} count={},", processNo, extractTasks.size());
            atomicProcessNo.set(processNo);

            // taskCountMap is used to count the number of tasks in table fragment query
            Map<String, Integer> taskCountMap = new HashMap<>(Constants.InitialCapacity.EMPTY);
            taskList.forEach(task -> {
                if (!taskCountMap.containsKey(task.getTableName())) {
                    taskCountMap.put(task.getTableName(), task.getDivisionsTotalNumber());
                }
            });
            // Initialization data extraction task execution status
            TableExtractStatusCache.init(taskCountMap);
        } else {
            log.error("process={} is running extract task , {} please wait ... ", atomicProcessNo.get(), processNo);
            throw new ProcessMultipleException("process {" + atomicProcessNo.get() + "} is running extract task");
        }
    }

    private void updateSinkMetadata(ExtractTask extractTask) {
        final String tableName = extractTask.getTableName();
        extractTask.setTableMetadata(metaDataService.getMetaDataOfSchemaByCache(tableName));
    }

    /**
     * Clean up the current build task
     */
    @Override
    public void cleanBuildTask() {
        if (Objects.nonNull(taskReference.getAcquire())) {
            taskReference.getAcquire().clear();
        }
        TableExtractStatusCache.removeAll();
        atomicProcessNo.set(PROCESS_NO_RESET);
        log.info("clear the current build task cache!");
        log.info("clear extraction service status flag!");
    }

    /**
     * Query the data extraction related information of the specified table under the current execution process
     *
     * @param tableName tableName
     * @return Table data extraction related information
     */
    @Override
    public ExtractTask queryTableInfo(String tableName) {
        ExtractTask extractTask = null;
        List<ExtractTask> taskList = taskReference.get();
        if (CollectionUtils.isEmpty(taskList)) {
            throw new TaskNotFoundException(tableName);
        }
        for (ExtractTask task : taskList) {
            if (Objects.equals(task.getTableName(), tableName)) {
                extractTask = task;
                break;
            }
        }
        if (Objects.isNull(extractTask)) {
            throw new TaskNotFoundException(tableName);
        }
        return extractTask;
    }

    /**
     * <pre>
     * Execute the data extraction task of the specified process number.
     *
     * Execute the extraction task, verify the current process number, and verify the extraction task.
     * For the verification of the extraction task, the polling method is used for multiple verifications.
     * Because the extraction execution logic of the source side and the destination side is asynchronous
     * and belongs to different Java processes.
     * In order to ensure the consistency of process data status between different processes,
     * polling method is adopted for multiple confirmation.
     * If the data in {@code taskReference} cannot be obtained after multiple confirmations,
     * an exception {@link org.opengauss.datachecker.common.exception.TaskNotFoundException} will be thrown
     * </pre>
     *
     * @param processNo Execution process number
     * @throws TaskNotFoundException If the task data is empty, an exception TaskNotFoundException will be thrown
     */
    @Async
    @Override
    public void execExtractTaskAllTables(String processNo) throws TaskNotFoundException {
        if (Objects.equals(atomicProcessNo.get(), processNo)) {
            int sleepCount = 0;
            while (CollectionUtils.isEmpty(taskReference.get())) {
                ThreadUtil.sleep(MAX_SLEEP_MILLIS_TIME);
                if (sleepCount++ > MAX_SLEEP_COUNT) {
                    log.info("endpoint [{}] and process[{}}] task is empty!",
                        extractProperties.getEndpoint().getDescription(), processNo);
                    break;
                }
            }
            kafkaAdminService.init(processNo, extractProperties.getEndpoint());
            dynamicThreadPoolManager.dynamicThreadPoolMonitor();
            List<ExtractTask> taskList = taskReference.get();
            if (CollectionUtils.isEmpty(taskList)) {
                return;
            }
            final ExecutorService executorService = dynamicThreadPoolManager.getExecutor(EXTRACT_EXECUTOR);
            Map<String, Integer> tableCheckStatus = checkingFeignClient.queryTableCheckStatus();
            taskList.forEach(task -> {
                log.info("Perform data extraction tasks {}", task.getTaskName());
                final String tableName = task.getTableName();
                if (!tableCheckStatus.containsKey(tableName) || tableCheckStatus.get(tableName) == -1) {
                    log.info("Abnormal table[{}] status, ignoring the current table data extraction task", tableName);
                    return;
                }
                ThreadUtil.requestConflictingSleeping();
                registerTopic(task);
                while (!topicCache.canCreateTopic(maximumTopicSize)) {
                    ThreadUtil.sleep(1000);
                }
                Topic topic = task.getTopic();
                Endpoint endpoint = extractProperties.getEndpoint();
                logKafka.info("try to create [{}] [{}]", topic.getTopicName(endpoint), topic.getPartitions());
                kafkaAdminService.createTopic(topic.getTopicName(endpoint), topic.getPartitions());
                topicCache.add(topic);
                log.info("current topic cache size = {}", topicCache.size());
                executorService.submit(new ExtractTaskRunnable(processNo, task, extractThreadSupport));
            });
        }
    }

    private void registerTopic(ExtractTask task) {
        int topicPartitions = TopicUtil.calcPartitions(task.getDivisionsTotalNumber());
        Endpoint currentEndpoint = extractProperties.getEndpoint();
        Topic topic = checkingFeignClient.registerTopic(task.getTableName(), topicPartitions, currentEndpoint);
        task.setTopic(topic);
    }

    @Override
    public List<String> buildRepairStatementUpdateDml(String schema, String tableName, boolean ogCompatibility,
        Set<String> diffSet) {
        repairStatementLog(schema, tableName, DML.REPLACE, diffSet.size());
        if (CollectionUtils.isEmpty(diffSet)) {
            return new ArrayList<>();
        }
        final TableMetadata metadata = metaDataService.getMetaDataOfSchemaByCache(tableName);
        return dataManipulationService.buildReplace(schema, tableName, diffSet, metadata, ogCompatibility);
    }

    @Override
    public List<String> buildRepairStatementInsertDml(String schema, String tableName, boolean ogCompatibility,
        Set<String> diffSet) {
        repairStatementLog(schema, tableName, DML.INSERT, diffSet.size());
        if (CollectionUtils.isEmpty(diffSet)) {
            return new ArrayList<>();
        }
        final TableMetadata metadata = metaDataService.getMetaDataOfSchemaByCache(tableName);
        return dataManipulationService.buildInsert(schema, tableName, diffSet, metadata, ogCompatibility);
    }

    @Override
    public List<String> buildRepairStatementDeleteDml(String schema, String tableName, boolean ogCompatibility,
        Set<String> diffSet) {
        repairStatementLog(schema, tableName, DML.DELETE, diffSet.size());
        final TableMetadata metadata = metaDataService.getMetaDataOfSchemaByCache(tableName);
        if (Objects.nonNull(metadata)) {
            final List<ColumnsMetaData> primaryMetas = metadata.getPrimaryMetas();
            return dataManipulationService.buildDelete(schema, tableName, diffSet, primaryMetas, ogCompatibility);
        } else {
            throw new BuildRepairStatementException(tableName);
        }
    }

    private void repairStatementLog(String schema, String tableName, DML dml, int count) {
        log.info("check table[{}.{}] repair [{}] diff-count={} build repair dml", schema, tableName, dml, count);
    }

    /**
     * Query table data
     *
     * @param tableName     tableName
     * @param compositeKeys Review primary key set
     * @return Primary key corresponds to table data
     */
    @Override
    public List<Map<String, String>> queryTableColumnValues(String tableName, List<String> compositeKeys) {
        final TableMetadata metadata = metaDataService.getMetaDataOfSchemaByCache(tableName);
        if (Objects.isNull(metadata)) {
            throw new TableNotExistException(tableName);
        }
        return dataManipulationService.queryColumnValues(tableName, new ArrayList<>(compositeKeys), metadata);
    }

    /**
     * Query the metadata information of the current table structure and perform hash calculation
     *
     * @param tableName tableName
     * @return Table structure hash
     */
    @Override
    public TableMetadataHash queryTableMetadataHash(String tableName) {
        return dataManipulationService.queryTableMetadataHash(tableName);
    }

    /**
     * PK list data is specified in the query table, and hash is used for secondary verification data query
     *
     * @param dataLog data log
     * @return row data hash
     */
    @Override
    public List<RowDataHash> querySecondaryCheckRowData(SourceDataLog dataLog) {
        final String tableName = dataLog.getTableName();
        final List<String> compositeKeys = dataLog.getCompositePrimaryValues();
        final TableMetadata metadata = metaDataService.getMetaDataOfSchemaByCache(tableName);
        if (Objects.isNull(metadata)) {
            throw new TableNotExistException(tableName);
        }
        if (compositeKeys.size() > MAX_QUERY_PAGE_SIZE) {
            List<RowDataHash> result = new ArrayList<>();
            AtomicInteger cnt = new AtomicInteger(0);
            List<String> tempCompositeKeys = new ArrayList<>();
            compositeKeys.forEach(key -> {
                tempCompositeKeys.add(key);
                if (cnt.incrementAndGet() % MAX_QUERY_PAGE_SIZE == 0) {
                    result
                        .addAll(dataManipulationService.queryColumnHashValues(tableName, tempCompositeKeys, metadata));
                    tempCompositeKeys.clear();
                }
            });
            if (CollectionUtils.isNotEmpty(tempCompositeKeys)) {
                result.addAll(dataManipulationService.queryColumnHashValues(tableName, tempCompositeKeys, metadata));
            }
            return result;
        } else {
            return dataManipulationService.queryColumnHashValues(tableName, compositeKeys, metadata);
        }
    }

    @Override
    public ExtractConfig getEndpointConfig() {
        ExtractConfig config = new ExtractConfig();
        final Database database = new Database();
        database.setDatabaseType(extractProperties.getDatabaseType()).setSchema(extractProperties.getSchema())
                .setEndpoint(extractProperties.getEndpoint());
        config.setDebeziumEnable(extractProperties.isDebeziumEnable()).setDatabase(database);
        return config;
    }

    @Override
    public Boolean isCheckTableEmpty(boolean isForced) {
        return metaDataService.isCheckTableEmpty(isForced);
    }

    @Override
    public TableMetadata queryIncrementMetaData(String tableName) {
        return metaDataService.queryIncrementMetaData(tableName);
    }
}
