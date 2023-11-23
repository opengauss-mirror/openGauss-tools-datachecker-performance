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

import com.alibaba.druid.pool.DruidDataSource;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.service.ProcessLogService;
import org.opengauss.datachecker.extract.cache.TopicCache;
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
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.sql.DataSource;

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
    private TopicCache topicCache;

    /**
     * create slice kafka agents
     *
     * @param slice slice
     * @return slice kafka agents
     */
    public SliceKafkaAgents createSliceKafkaAgents(SliceVo slice) {
        return createSliceKafkaAgents(slice.getTable(), slice.getPtn());
    }

    public void saveProcessing(SliceVo slice) {
        processLogService.saveProcessHistoryLogging(slice.getTable(), slice.getNo());
    }

    public SliceKafkaAgents createSliceKafkaAgents(String table, int ptn) {
        Topic topic = topicCache.getTopic(table);
        String topicName = topic.getTopicName(ConfigCache.getEndPoint());
        return new SliceKafkaAgents(kafkaTemplate, kafkaConsumerConfig.createConsumer(), topicName, ptn);
    }

    public SliceKafkaAgents createSliceKafkaAgents(String table) {
        Topic topic = topicCache.getTopic(table);
        return new SliceKafkaAgents(kafkaTemplate, kafkaConsumerConfig.createConsumer(), topic);
    }

    /**
     * get table table metadata
     *
     * @param table table name
     * @return table metadata
     */
    public TableMetadata getTableMetaData(String table) {
        return baseDataService.queryTableMetadata(table);
    }

    /**
     * ceeate jdbc data operations
     *
     * @param dataSource datasource
     * @return JdbcDataOperations
     */
    public JdbcDataOperations getJdbcDataOperations(DataSource dataSource) {
        return new JdbcDataOperations(dataSource, resourceManager);
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

    public AutoSliceQueryStatement createAutoSliceQueryStatement(TableMetadata tableMetadata) {
        CheckPoint checkPoint = new CheckPoint(baseDataService.getDataAccessService());
        return factory.createSliceQueryStatement(checkPoint, tableMetadata);
    }

    /**
     * feedback slice status
     *
     * @param sliceExtend slice extend
     */
    @Retryable(maxAttempts = 3)
    public void feedbackStatus(SliceExtend sliceExtend) {
        checkingFeignClient.refreshRegisterSlice(sliceExtend);
    }

    /**
     * create  MemoryOperations instance
     *
     * @return MemoryOperations
     */
    public MemoryOperations getMemoryDataOperations() {
        return new MemoryOperations(resourceManager);
    }
}
