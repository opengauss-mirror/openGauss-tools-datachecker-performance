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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.constant.Constants;
import org.opengauss.datachecker.common.entry.common.CheckPointData;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.Database;
import org.opengauss.datachecker.common.entry.extract.ExtractConfig;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.TableMetadataHash;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.exception.ProcessMultipleException;
import org.opengauss.datachecker.common.exception.TableNotExistException;
import org.opengauss.datachecker.common.exception.TaskNotFoundException;
import org.opengauss.datachecker.common.service.DynamicThreadPoolManager;
import org.opengauss.datachecker.common.util.IdGenerator;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.common.util.TopicUtil;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.cache.TableCheckPointCache;
import org.opengauss.datachecker.extract.cache.TableExtractStatusCache;
import org.opengauss.datachecker.extract.cache.TopicCache;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.config.KafkaConsumerConfig;
import org.opengauss.datachecker.extract.data.BaseDataService;
import org.opengauss.datachecker.extract.data.access.DataAccessService;
import org.opengauss.datachecker.extract.kafka.KafkaAdminService;
import org.opengauss.datachecker.extract.slice.ExtractPointSwapManager;
import org.opengauss.datachecker.extract.slice.SliceRegister;
import org.opengauss.datachecker.extract.slice.factory.SliceFactory;
import org.opengauss.datachecker.extract.task.CheckPoint;
import org.opengauss.datachecker.extract.task.DataManipulationService;
import org.opengauss.datachecker.extract.task.ExtractTaskBuilder;
import org.opengauss.datachecker.extract.task.ExtractThreadSupport;
import org.opengauss.datachecker.extract.task.sql.AutoSliceQueryStatement;
import org.opengauss.datachecker.extract.task.sql.QueryStatementFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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
@Service
public class DataExtractServiceImpl implements DataExtractService {
    private static final Logger log = LogUtils.getLogger();
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
    private static final String TASK_NAME_PREFIX = "extract_task_";
    private static final int SINGLE_SLICE_NUM = 1;

    /**
     * After the service is started, the {code atomicProcessNo} attribute will be initialized,
     * <p>
     * When the user starts the verification process, the {code atomicProcessNo} attribute will be verified and set
     */
    private final AtomicReference<String> atomicProcessNo = new AtomicReference<>(PROCESS_NO_RESET);

    private final AtomicReference<List<ExtractTask>> taskReference = new AtomicReference<>();
    private final QueryStatementFactory factory = new QueryStatementFactory();
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
    @Autowired
    private DataAccessService dataAccessService;
    @Autowired
    private BaseDataService baseDataService;
    @Resource
    private TableCheckPointCache tableCheckPointCache;
    @Resource
    private DynamicThreadPoolManager dynamicThreadPoolManager;
    @Resource
    private KafkaAdminService kafkaAdminService;
    @Resource
    private SliceRegister sliceRegister;
    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;
    @Resource
    private KafkaConsumerConfig kafkaConsumerConfig;
    private ExtractPointSwapManager checkPointManager = null;

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
        TopicCache.initEndpoint(extractProperties.getEndpoint());
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
            final List<ExtractTask> extractTasks = taskList.stream()
                                                           .filter(task -> tableNames.contains(task.getTableName()))
                                                           .collect(Collectors.toList());
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
            taskReference.getAcquire()
                         .clear();
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
                    log.info("endpoint [{}] and process[{}}] task is empty!", extractProperties.getEndpoint()
                                                                                               .getDescription(),
                        processNo);
                    break;
                }
            }
            ConfigCache.put(ConfigConstants.PROCESS_NO, processNo);
            List<ExtractTask> taskList = taskReference.get();
            if (CollectionUtils.isEmpty(taskList)) {
                return;
            }
            dynamicThreadPoolManager.dynamicThreadPoolMonitor();
            sliceRegister.startCheckPointMonitor();
            Map<String, Integer> tableCheckStatus = checkingFeignClient.queryTableCheckStatus();
            int maximumTopicSize = ConfigCache.getIntValue(ConfigConstants.MAXIMUM_TOPIC_SIZE);
            tableRegisterCheckPoint(taskList);
            taskList.forEach(task -> execExtractTableTask(tableCheckStatus, maximumTopicSize, task));
        }
    }

    private void execExtractTableTask(Map<String, Integer> tableCheckStatus, int maximumTopicSize, ExtractTask task) {
        try {
            log.info("Perform data extraction tasks {}", task.getTaskName());
            final String tableName = task.getTableName();
            if (!tableCheckStatus.containsKey(tableName) || tableCheckStatus.get(tableName) == -1) {
                log.warn("Abnormal table[{}] status, ignoring the current table data extraction task", tableName);
                return;
            }
            registerTopic(task);
            while (!TopicCache.canCreateTopic(maximumTopicSize)) {
                ThreadUtil.sleepHalfSecond();
            }
            Topic topic = task.getTopic();
            Endpoint endpoint = extractProperties.getEndpoint();
            log.info("try to create [{}] [{}]", topic.getTopicName(endpoint), topic.getPtnNum());
            kafkaAdminService.createTopic(topic.getTopicName(endpoint), topic.getPtnNum());
            TopicCache.add(topic);
            while (!tableCheckPointCache.getAll()
                                        .containsKey(tableName)) {
                ThreadUtil.sleepHalfSecond();
            }
            List<Object> summarizedCheckPoint = tableCheckPointCache.get(tableName);
            log.debug("table [{}] summarized check-point-list : {}", tableName, summarizedCheckPoint);
            List<SliceVo> sliceVoList =
                buildSliceByTask(summarizedCheckPoint, task.getTableMetadata(), topic, endpoint);
            log.info("table [{}] have {} slice to check", tableName, sliceVoList.size());
            addSliceProcessor(sliceVoList);
        } catch (Exception ex) {
            log.error("async exec extract tables error {}:{} ", task.getTableName(), ex.getMessage(), ex);
        }
    }

    private List<SliceVo> buildSliceByTask(List<Object> summarizedCheckPoint, TableMetadata tableMetadata, Topic topic,
        Endpoint endpoint) {
        List<SliceVo> sliceVoList;
        if (noTableSlice(tableMetadata, summarizedCheckPoint)) {
            sliceVoList = buildSingleSlice(tableMetadata, topic, endpoint);
        } else {
            sliceVoList = buildSlice(summarizedCheckPoint, tableMetadata, topic, endpoint);
        }
        return sliceVoList;
    }

    private void addSliceProcessor(List<SliceVo> sliceVoList) {
        SliceFactory sliceFactory = new SliceFactory(baseDataService.getDataSource());
        sliceRegister.batchRegister(sliceVoList);
        if (sliceVoList.size() <= 20) {
            ExecutorService executorService = dynamicThreadPoolManager.getExecutor(EXTRACT_EXECUTOR);
            log.debug("table [{}] get executorService success", sliceVoList.get(0)
                                                                           .getTable());
            sliceVoList.forEach(sliceVo -> executorService.submit(sliceFactory.createSliceProcessor(sliceVo)));
        } else {
            int topicSize = ConfigCache.getIntValue(ConfigConstants.MAXIMUM_TOPIC_SIZE);
            int extendMaxPoolSize = ConfigCache.getIntValue(ConfigConstants.EXTEND_MAXIMUM_POOL_SIZE);
            ExecutorService extendExecutor = dynamicThreadPoolManager.getFreeExecutor(topicSize, extendMaxPoolSize);
            log.debug("table [{}] get extendExecutor success", sliceVoList.get(0)
                                                                          .getTable());
            sliceVoList.forEach(sliceVo -> extendExecutor.submit(sliceFactory.createSliceProcessor(sliceVo)));
        }
    }

    private List<SliceVo> buildSingleSlice(TableMetadata metadata, Topic topic, Endpoint endpoint) {
        SliceVo sliceVo = new SliceVo();
        sliceVo.setNo(SINGLE_SLICE_NUM);
        sliceVo.setTable(metadata.getTableName());
        sliceVo.setSchema(metadata.getSchema());
        sliceVo.setName(sliceTaskNameBuilder(metadata.getTableName(), 0));
        sliceVo.setPtnNum(topic.getPtnNum());
        sliceVo.setTotal(SINGLE_SLICE_NUM);
        sliceVo.setEndpoint(endpoint);
        sliceVo.setFetchSize(ConfigCache.getIntValue(ConfigConstants.MAXIMUM_TABLE_SLICE_SIZE));
        return List.of(sliceVo);
    }

    private boolean noTableSlice(TableMetadata tableMetadata, List<Object> summarizedCheckPoint) {
        return summarizedCheckPoint.size() <= 2 || getQueryDop() == 1 || tableMetadata.getConditionLimit() != null;
    }

    private int getQueryDop() {
        return ConfigCache.getIntValue(ConfigConstants.QUERY_DOP);
    }

    private List<SliceVo> buildSlice(List<Object> summarizedCheckPoint, TableMetadata metadata, Topic topic,
        Endpoint endpoint) {
        int topicPartitions = topic.getPtnNum();
        ArrayList<SliceVo> sliceTaskList = new ArrayList<>();
        Iterator<Object> iterator = summarizedCheckPoint.iterator();
        Object preOffset = iterator.next();
        int index = 0;
        while (iterator.hasNext()) {
            Object offset = iterator.next();
            SliceVo sliceVo = new SliceVo();
            sliceVo.setTable(metadata.getTableName());
            sliceVo.setSchema(metadata.getSchema());
            sliceVo.setFetchSize(ConfigCache.getIntValue(ConfigConstants.MAXIMUM_TABLE_SLICE_SIZE));
            sliceVo.setName(sliceTaskNameBuilder(metadata.getTableName(), index));
            sliceVo.setBeginIdx(String.valueOf(preOffset));
            sliceVo.setEndIdx(String.valueOf(offset));
            sliceVo.setPtnNum(topic.getPtnNum());
            sliceVo.setPtn(index % topicPartitions);
            sliceVo.setTotal(summarizedCheckPoint.size() - 1);
            sliceVo.setEndpoint(endpoint);
            sliceVo.setNo(++index);
            sliceTaskList.add(sliceVo);
            preOffset = offset;
        }
        return sliceTaskList;
    }

    private List<Object> getCheckPoint(CheckPoint checkPoint, TableMetadata metadata) {
        AutoSliceQueryStatement sliceStatement = factory.createSliceQueryStatement(checkPoint, metadata);
        List<Object> checkPointList =
            sliceStatement.getCheckPoint(metadata, ConfigCache.getIntValue(ConfigConstants.MAXIMUM_TABLE_SLICE_SIZE));
        if (CollectionUtils.isEmpty(checkPointList)) {
            return new ArrayList<>();
        }
        return checkPointList;
    }

    private void tableRegisterCheckPoint(List<ExtractTask> taskList) {
        new Thread(() -> {
            KafkaConsumer<String, String> consumer = kafkaConsumerConfig.createConsumer(IdGenerator.nextId36());
            checkPointManager = new ExtractPointSwapManager(kafkaTemplate, consumer);
            checkPointManager.setCheckPointSwapTopicName(ConfigCache.getValue(ConfigConstants.PROCESS_NO));
            log.info("tableRegisterCheckPoint start pollSwapPoint thread");
            checkPointManager.pollSwapPoint(tableCheckPointCache);
            taskList.forEach(this::registerCheckPoint);
            while (tableCheckPointCache.tableCount() != taskList.size()) {
                ThreadUtil.sleepHalfSecond();
            }
            checkPointManager.close();
            sliceRegister.stopCheckPointMonitor(ConfigCache.getEndPoint());
        }).start();
    }

    private void registerCheckPoint(ExtractTask task) {
        String tableName = task.getTableName();
        log.debug("register check point [{}]", tableName);
        CheckPoint checkPoint = new CheckPoint(dataAccessService);
        List<Object> checkPointList = getCheckPoint(checkPoint, task.getTableMetadata());
        if (checkPointList == null || checkPointList.size() <= 2) {
            checkPointList = List.of();
            tableCheckPointCache.put(tableName, checkPointList);
        }
        checkPointManager.send(new CheckPointData().setTableName(tableName)
                                                   .setDigit(checkPoint.checkPkNumber(task.getTableMetadata()))
                                                   .setCheckPointList(checkPointList));
    }

    private String sliceTaskNameBuilder(@NonNull String tableName, int index) {
        return TASK_NAME_PREFIX.concat(tableName)
                               .concat("_slice_")
                               .concat(String.valueOf(index + 1));
    }

    private void registerTopic(ExtractTask task) {
        int topicPartitions = TopicUtil.calcPartitions(task.getDivisionsTotalNumber());
        Endpoint currentEndpoint = extractProperties.getEndpoint();
        Topic topic;
        if (Objects.equals(Endpoint.SOURCE, currentEndpoint)) {
            topic = checkingFeignClient.sourceRegisterTopic(task.getTableName(), topicPartitions);
        } else {
            topic = checkingFeignClient.sinkRegisterTopic(task.getTableName(), topicPartitions);
        }
        task.setTopic(topic);
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
                    result.addAll(
                        dataManipulationService.queryColumnHashValues(tableName, tempCompositeKeys, metadata));
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
        BeanUtils.copyProperties(extractProperties, database);
        BeanUtils.copyProperties(extractProperties, config);
        config.setDatabase(database);
        return config;
    }

    @Override
    public TableMetadata queryIncrementMetaData(String tableName) {
        return metaDataService.queryIncrementMetaData(tableName);
    }
}
