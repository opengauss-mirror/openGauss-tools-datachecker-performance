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

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.Constants;
import org.opengauss.datachecker.common.entry.common.CheckPointData;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.util.IdGenerator;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.cache.TableCheckPointCache;
import org.opengauss.datachecker.extract.config.KafkaConsumerConfig;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author ：wangchao
 * @date ：Created in 2023/11/16
 * @since ：11
 */
public class ExtractPointSwapManager {
    private static final Logger log = LogUtils.getLogger(ExtractPointSwapManager.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaConsumer<String, String> kafkaConsumer;
    private String checkPointSwapTopicName = null;
    private Endpoint endpoint;
    private boolean isCompletedSwapTablePoint = false;
    private ExecutorService executorService;
    private KafkaConsumerConfig kafkaConsumerConfig;

    public ExtractPointSwapManager(KafkaTemplate<String, String> kafkaTemplate,
        KafkaConsumerConfig kafkaConsumerConfig) {
        this.kafkaTemplate = kafkaTemplate;
        this.endpoint = ConfigCache.getEndPoint();
        this.endpoint = ConfigCache.getEndPoint();
        this.executorService = ThreadUtil.newSingleThreadExecutor();
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        this.kafkaConsumer = kafkaConsumerConfig.createConsumer(IdGenerator.nextId36());
    }

    public void send(CheckPointData checkPointData) {
        checkPointData.setEndpoint(endpoint);
        kafkaTemplate.send(checkPointSwapTopicName, endpoint.getDescription(), JSONObject.toJSONString(checkPointData));
        LogUtils.info(log, "send check point [{}][{}]", checkPointSwapTopicName, checkPointData);
    }

    public void pollSwapPoint(TableCheckPointCache tableCheckPointCache) {
        executorService.submit(() -> {
            trySubscribe();
            ConsumerRecords<String, String> records;
            AtomicInteger deliveredCount = new AtomicInteger();
            LogUtils.info(log, "pollSwapPoint thread started");
            while (!isCompletedSwapTablePoint) {
                try {
                    records = kafkaConsumer.poll(Duration.ofSeconds(1));
                    if (!records.isEmpty()) {
                        records.forEach(record -> {
                            if (Objects.equals(record.key(), Endpoint.CHECK.getDescription())) {
                                CheckPointData pointData = JSONObject.parseObject(record.value(), CheckPointData.class);
                                tableCheckPointCache.put(pointData.getTableName(), translateDigitPoint(pointData));
                                deliveredCount.getAndIncrement();
                                LogUtils.info(log, "swap summarized checkpoint of table [{}]:[{}] ", deliveredCount,
                                    pointData);
                            }
                        });
                    } else {
                        ThreadUtil.sleepOneSecond();
                    }
                } catch (Exception ex) {
                    if (Objects.equals("java.lang.InterruptedException", ex.getMessage())) {
                        LogUtils.warn(log, "kafka consumer stop by Interrupted");
                    } else {
                        LogUtils.error(log, "pollSwapPoint ", ex);
                    }
                }
            }
            LogUtils.warn(log, "close check point swap consumer {} :{}", checkPointSwapTopicName,
                kafkaConsumer.groupMetadata()
                             .groupId());
            kafkaConsumerConfig.closeConsumer(kafkaConsumer);
        });
    }

    private List<Object> translateDigitPoint(CheckPointData pointData) {
        return pointData.isDigit() ? pointData.getCheckPointList()
                                              .stream()
                                              .map(obj -> Long.parseLong((String) obj))
                                              .collect(Collectors.toList()) : pointData.getCheckPointList();
    }

    private void trySubscribe() {
        int subscribeTimes = 1;
        boolean isSubscribe = false;
        while (!isSubscribe && subscribeTimes < 5) {
            isSubscribe = subscribe();
            subscribeTimes++;
        }
    }

    private boolean subscribe() {
        boolean isSubscribe = false;
        try {
            kafkaConsumer.subscribe(List.of(checkPointSwapTopicName));
            Map<String, List<PartitionInfo>> listTopics = kafkaConsumer.listTopics();
            isSubscribe = listTopics.containsKey(checkPointSwapTopicName);
        } catch (Exception ex) {
            LogUtils.warn(log, "subscribe {} failed", checkPointSwapTopicName);
        }
        return isSubscribe;
    }

    public void setCheckPointSwapTopicName(String process) {
        this.checkPointSwapTopicName = String.format(Constants.SWAP_POINT_TOPIC_TEMP, process);
        LogUtils.info(log, "check point swap topic {}", checkPointSwapTopicName);
    }

    public void close() {
        this.isCompletedSwapTablePoint = true;

        this.executorService.shutdownNow();
    }
}
