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

package org.opengauss.datachecker.extract.slice;

import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.service.ProcessLogService;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.config.KafkaConsumerConfig;
import org.opengauss.datachecker.extract.data.BaseDataService;
import org.opengauss.datachecker.extract.resource.JdbcDataOperations;
import org.opengauss.datachecker.extract.resource.MemoryOperations;
import org.opengauss.datachecker.extract.resource.ResourceManager;
import org.opengauss.datachecker.extract.slice.common.SliceKafkaAgents;
import org.opengauss.datachecker.extract.task.CheckPoint;
import org.opengauss.datachecker.extract.task.sql.AutoSliceQueryStatement;
import org.opengauss.datachecker.extract.task.sql.FullQueryStatement;
import org.opengauss.datachecker.extract.task.sql.QueryStatementFactory;
import org.opengauss.datachecker.extract.task.sql.SliceQueryStatement;
import org.opengauss.datachecker.extract.task.sql.UnionPrimarySliceQueryStatement;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import jakarta.annotation.Resource;

import java.util.Objects;
import java.util.concurrent.Future;

/**
 * SliceProcessorContext
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/8
 * @since ：11
 */
@Component
public class SliceProcessorContext {
    private final QueryStatementFactory factory = new QueryStatementFactory();
    @Resource
    private ProcessLogService processLogService;
    @Resource
    private ResourceManager resourceManager;
    @Resource
    private BaseDataService baseDataService;
    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;
    @Resource
    private KafkaConsumerConfig kafkaConsumerConfig;
    @Resource
    private CheckingFeignClient checkingFeignClient;
    @Resource
    private ThreadPoolTaskExecutor sliceSendExecutor;
    private SliceStatusFeedbackService sliceStatusFeedbackService;

    /**
     * save processing
     *
     * @param slice slice
     */
    public void saveProcessing(SliceVo slice) {
        processLogService.saveProcessHistoryLogging(slice.getTable(), slice.getTotal(), slice.getNo());
    }

    /**
     * async thread add threadPool
     *
     * @param sliceSendRunnable sliceSendRunnable
     * @return future
     */
    public Future<?> asyncSendSlice(Runnable sliceSendRunnable) {
        return sliceSendExecutor.submit(sliceSendRunnable);
    }

    /**
     * 销毁kafkaTemplate
     */
    @PreDestroy
    public void destroy() {
        kafkaTemplate.destroy();
    }

    /**
     * 创建分片kafka代理
     *
     * @param topicName topic 名称
     * @param groupId GroupID
     * @return 分片kafka代理
     */
    public SliceKafkaAgents createSliceFixedKafkaAgents(String topicName, String groupId) {
        return new SliceKafkaAgents(kafkaTemplate, kafkaConsumerConfig.createConsumer(groupId), topicName, 0);
    }

    /**
     * get table table metadata
     *
     * @param table table name
     * @return table metadata
     */
    public TableMetadata getTableMetaData(String table) {
        return MetaDataCache.get(table);
    }

    /**
     * ceeate jdbc data operations
     *
     * @return JdbcDataOperations
     */
    public JdbcDataOperations getJdbcDataOperations() {
        return new JdbcDataOperations(resourceManager);
    }

    /**
     * create  FullQueryStatement instance
     *
     * @return FullQueryStatement
     */
    public FullQueryStatement createFullQueryStatement() {
        return factory.createFullQueryStatement();
    }

    /**
     * create SliceQueryStatement instance
     *
     * @return SliceQueryStatement
     */
    public SliceQueryStatement createSliceQueryStatement() {
        return factory.createSliceQueryStatement();
    }

    /**
     * create slice query statement of union primary slice
     *
     * @return UnionPrimarySliceQueryStatement
     */
    public UnionPrimarySliceQueryStatement createSlicePageQueryStatement() {
        return factory.createSlicePageQueryStatement();
    }

    /**
     * create AutoSliceQueryStatement instance
     *
     * @param tableName table name
     * @return AutoSliceQueryStatement
     */
    public AutoSliceQueryStatement createAutoSliceQueryStatement(String tableName) {
        CheckPoint checkPoint = new CheckPoint(baseDataService.getDataAccessService());
        return factory.createSliceQueryStatement(checkPoint, tableName);
    }

    /**
     * feedback slice status
     *
     * @param sliceExtend slice extend
     */
    public void feedbackStatus(SliceExtend sliceExtend) {
        sliceStatusFeedbackService.addFeedbackStatus(sliceExtend);
    }

    /**
     * create  MemoryOperations instance
     *
     * @return MemoryOperations
     */
    public MemoryOperations getMemoryDataOperations() {
        return new MemoryOperations(resourceManager);
    }

    public void startSliceStatusFeedbackService() {
        sliceStatusFeedbackService = new SliceStatusFeedbackService(checkingFeignClient);
        sliceStatusFeedbackService.feedback();
    }

    public void shutdownSliceStatusFeedbackService() {
        if (Objects.nonNull(sliceStatusFeedbackService)) {
            sliceStatusFeedbackService.stop();
        }
    }
}
