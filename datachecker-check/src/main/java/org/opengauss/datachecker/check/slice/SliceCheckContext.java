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

package org.opengauss.datachecker.check.slice;

import org.apache.kafka.common.TopicPartition;
import org.opengauss.datachecker.check.cache.TopicRegister;
import org.opengauss.datachecker.check.event.KafkaTopicDeleteProvider;
import org.opengauss.datachecker.check.modules.check.CheckDiffResult;
import org.opengauss.datachecker.check.modules.check.KafkaConsumerHandler;
import org.opengauss.datachecker.check.modules.check.KafkaConsumerService;
import org.opengauss.datachecker.check.modules.report.SliceCheckResultManager;
import org.opengauss.datachecker.check.modules.report.SliceProgressService;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * SliceCheckContext
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/2
 * @since ：11
 */
@Component
public class SliceCheckContext {
    @Resource
    private TopicRegister topicRegister;
    @Resource
    private KafkaConsumerService kafkaConsumerService;
    @Resource
    private SliceProgressService sliceProgressService;
    @Resource
    private SliceCheckResultManager sliceCheckResultManager;
    @Resource
    private KafkaTopicDeleteProvider kafkaTopicDeleteProvider;

    /**
     * create kafka consumer handler
     *
     * @return KafkaConsumerHandler
     */
    public KafkaConsumerHandler buildKafkaHandler() {
        return new KafkaConsumerHandler(kafkaConsumerService.buildKafkaConsumer(true),
            kafkaConsumerService.getRetryFetchRecordTimes());
    }

    /**
     * get source or sink table topic
     *
     * @param table    table
     * @param endpoint source or sink
     * @return topic name
     */
    public String getTopicName(String table, Endpoint endpoint) {
        Topic topic = topicRegister.getTopic(table);
        return topic.getTopicName(endpoint);
    }

    /**
     * get source and sink table topics
     *
     * @param table table
     * @return topics
     */
    public String getTopicName(String table) {
        Topic topic = topicRegister.getTopic(table);
        return topic.toTopicString();
    }

    /**
     * build topic partition of tables
     *
     * @param table    table
     * @param endpoint endpoint
     * @param ptn      partition no
     * @return topic partition
     */
    public TopicPartition getTopic(String table, Endpoint endpoint, int ptn) {
        Topic topic = topicRegister.getTopic(table);
        return new TopicPartition(topic.getTopicName(endpoint), ptn);
    }

    public Topic getTopic(String table) {
        return topicRegister.getTopic(table);
    }

    /**
     * refresh slice check progress
     *
     * @param slice    slice
     * @param rowCount slice of row count
     */
    public void refreshSliceCheckProgress(SliceVo slice, long rowCount) {
        sliceProgressService.updateProgress(slice.getTable(), slice.getTotal(), slice.getNo(), rowCount);
    }

    /**
     * add slice check Result
     *
     * @param slice  slice
     * @param result check result
     */
    public void addCheckResult(SliceVo slice, CheckDiffResult result) {
        sliceCheckResultManager.addResult(slice, result);
    }

    /**
     * get table Metadata
     *
     * @param table table
     * @return table metadata
     */
    public TableMetadata getTableMetaData(String table) {
        return null;
    }

    public void addTableStructureDiffResult(SliceVo slice, CheckDiffResult result) {
        sliceCheckResultManager.addTableStructureDiffResult(slice, result);
    }

    public void dropTableTopics(String table) {
        kafkaTopicDeleteProvider.addTableToDropTopic(table, true);
    }
}
