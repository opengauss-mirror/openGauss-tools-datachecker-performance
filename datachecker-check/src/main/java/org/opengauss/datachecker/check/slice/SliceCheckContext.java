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

import org.opengauss.datachecker.check.config.KafkaConsumerConfig;
import org.opengauss.datachecker.check.modules.check.CheckDiffResult;
import org.opengauss.datachecker.check.modules.check.KafkaConsumerHandler;
import org.opengauss.datachecker.check.modules.check.KafkaConsumerService;
import org.opengauss.datachecker.check.modules.report.SliceCheckResultManager;
import org.opengauss.datachecker.check.modules.report.SliceProgressService;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.service.ProcessLogService;
import org.opengauss.datachecker.common.util.TopicUtil;
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
    private KafkaConsumerService kafkaConsumerService;
    @Resource
    private KafkaConsumerConfig kafkaConsumerConfig;
    @Resource
    private SliceProgressService sliceProgressService;
    @Resource
    private SliceCheckResultManager sliceCheckResultManager;
    @Resource
    private ProcessLogService processLogService;

    /**
     * create kafka consumer handler
     *
     * @param groupId groupId
     * @return KafkaConsumerHandler
     */
    public synchronized KafkaConsumerHandler buildKafkaHandler(String groupId) {
        KafkaConsumerHandler kafkaHandler = new KafkaConsumerHandler(kafkaConsumerConfig.takeConsumer());
        return new KafkaConsumerHandler(kafkaConsumerConfig.takeConsumer());
    }

    /**
     * 从kafka消费者缓存池中，获取一个consumer，并创建kafka处理器对象
     *
     * @return KafkaConsumerHandler
     */
    public synchronized KafkaConsumerHandler createKafkaHandler() {
        return new KafkaConsumerHandler(kafkaConsumerConfig.takeConsumer());
    }

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
     * get consumer retry fetch record times
     *
     * @return duration times
     */
    public int getRetryFetchRecordTimes() {
        return kafkaConsumerService.getRetryFetchRecordTimes();
    }

    /**
     * get source or sink table topic
     *
     * @param table table
     * @param endpoint source or sink
     * @return topic name
     */
    public String getTopicName(String table, Endpoint endpoint) {
        String process = ConfigCache.getValue(ConfigConstants.PROCESS_NO);
        int maximumTopicSize = ConfigCache.getIntValue(ConfigConstants.MAXIMUM_TOPIC_SIZE);
        return TopicUtil.getMoreFixedTopicName(process, endpoint, table, maximumTopicSize);
    }

    /**
     * refresh slice check progress
     *
     * @param slice slice
     * @param rowCount slice of row count
     */
    public void refreshSliceCheckProgress(SliceVo slice, long rowCount) {
        sliceProgressService.updateProgress(slice.getTable(), slice.getTotal(), slice.getNo(), rowCount);
    }

    /**
     * add slice check Result
     *
     * @param slice slice
     * @param result check result
     */
    public void addCheckResult(SliceVo slice, CheckDiffResult result) {
        sliceCheckResultManager.addResult(slice, result);
    }

    public void addTableStructureDiffResult(SliceVo slice, CheckDiffResult result) {
        sliceCheckResultManager.addTableStructureDiffResult(slice, result);
    }

    public void saveProcessHistoryLogging(SliceVo slice) {
        processLogService.saveProcessHistoryLogging(slice.getTable(), slice.getNo());
    }

    /**
     * 归还当前kafka consumer 到消费者池
     *
     * @param consumer consumer
     */
    public void returnConsumer(KafkaConsumerHandler consumer) {
        kafkaConsumerConfig.returnConsumer(consumer.getConsumer());
    }
}
