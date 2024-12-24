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

import cn.hutool.core.bean.BeanUtil;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.Constants;
import org.opengauss.datachecker.common.entry.common.CheckPointBean;
import org.opengauss.datachecker.common.entry.common.CheckPointData;
import org.opengauss.datachecker.common.entry.common.PointPair;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.util.IdGenerator;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.cache.TableCheckPointCache;
import org.opengauss.datachecker.extract.config.KafkaConsumerConfig;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
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
    private static final int MAX_RETRY_TIMES = 100;

    private final Map<String, List<CheckPointBean>> tablePointCache = new ConcurrentHashMap<>();
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

    /**
     * send check point to check endpoint by kafka
     *
     * @param checkPointData check point
     */
    public void send(CheckPointData checkPointData) {
        checkPointData.setEndpoint(endpoint);
        List<PointPair> checkPointList = checkPointData.getCheckPointList();
        int size = checkPointList.size();
        if (size == 0) {
            CheckPointBean pointBean = new CheckPointBean();
            BeanUtil.copyProperties(checkPointData, pointBean);
            pointBean.setCheckPoint(null);
            pointBean.setSize(size);
            kafkaTemplate.send(checkPointSwapTopicName, endpoint.getDescription(), JSONObject.toJSONString(pointBean));
        } else {
            checkPointList.forEach(checkPoint -> {
                CheckPointBean pointBean = new CheckPointBean();
                BeanUtil.copyProperties(checkPointData, pointBean);
                pointBean.setSize(size);
                pointBean.setCheckPoint(checkPoint);
                sendMsg(checkPointSwapTopicName, endpoint.getDescription(), pointBean, 0);
            });
        }
        log.info("send checkPointData success, table :{} size:{}", checkPointData.getTableName(), size);
    }

    private void sendMsg(String topic, String key, CheckPointBean tmpBean, int reTryTimes) {
        try {
            kafkaTemplate.send(topic, key, JSONObject.toJSONString(tmpBean));
        } catch (TimeoutException ex) {
            if (reTryTimes > MAX_RETRY_TIMES) {
                log.error("{}send msg to kafka timeout, topic: {} key: {} reTryTimes: {}",
                    ErrorCode.SEND_SLICE_POINT_TIMEOUT, topic, key, reTryTimes);
            }
            ThreadUtil.sleepLongCircle(++reTryTimes);
            sendMsg(topic, key, tmpBean, reTryTimes);
        }
    }

    /**
     * poll check point from check endpoint by kafka; add check point in cache
     *
     * @param tableCheckPointCache cache
     */
    public void pollSwapPoint(TableCheckPointCache tableCheckPointCache) {
        executorService.submit(() -> {
            trySubscribe();
            ConsumerRecords<String, String> records;
            AtomicInteger deliveredCount = new AtomicInteger();
            LogUtils.info(log, "pollSwapPoint thread started");
            int retryTimesWait = 0;
            while (!isCompletedSwapTablePoint) {
                try {
                    records = kafkaConsumer.poll(Duration.ofSeconds(1));
                    if (!records.isEmpty()) {
                        records.forEach(record -> {
                            if (Objects.equals(record.key(), Endpoint.CHECK.getDescription())) {
                                CheckPointBean pointBean = JSONObject.parseObject(record.value(), CheckPointBean.class);
                                if (tablePointCache.containsKey(pointBean.getTableName())) {
                                    tablePointCache.get(pointBean.getTableName()).add(pointBean);
                                } else {
                                    List<CheckPointBean> list = new LinkedList<>();
                                    list.add(pointBean);
                                    tablePointCache.put(pointBean.getTableName(), list);
                                }
                            }
                        });
                        retryTimesWait = 0;
                    } else {
                        LogUtils.info(log, "wait swap summarized checkpoint of table {}...", ++retryTimesWait);
                        ThreadUtil.sleepCircle(retryTimesWait);
                    }
                    processTablePoint(tableCheckPointCache, deliveredCount);
                } catch (Exception ex) {
                    LogUtils.info(log, "pollSwapPoint swap summarized checkpoint end {}", ex.getMessage());
                }
            }
            LogUtils.warn(log, "close check point swap consumer {} :{}", checkPointSwapTopicName,
                kafkaConsumer.groupMetadata().groupId());
            kafkaConsumerConfig.closeConsumer(kafkaConsumer);
        });
    }

    private void processTablePoint(TableCheckPointCache tableCheckPointCache, AtomicInteger deliveredCount) {
        Iterator<Map.Entry<String, List<CheckPointBean>>> iterator = tablePointCache.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, List<CheckPointBean>> next = iterator.next();
            List<CheckPointBean> value = next.getValue();
            CheckPointBean checkPointBean = value.get(0);
            if (checkPointBean.getSize() == value.size()) {
                List<PointPair> collect = value.stream()
                    .map(CheckPointBean::getCheckPoint)
                    .collect(Collectors.toList());
                tableCheckPointCache.add(next.getKey(), collect);
                deliveredCount.getAndIncrement();
                iterator.remove();
                log.info("send checkpoint success, table: {}, size {}", next.getKey(), collect.size());
            }
        }
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
            LogUtils.info(log, "subscribe check point swap topic {} ", checkPointSwapTopicName);
        } catch (Exception ex) {
            LogUtils.warn(log, "subscribe {} failed", checkPointSwapTopicName);
        }
        return isSubscribe;
    }

    public void setCheckPointSwapTopicName(String process) {
        this.checkPointSwapTopicName = String.format(Constants.SWAP_POINT_TOPIC_TEMP, process);
    }

    /**
     * close check point swap thread
     */
    public void close() {
        this.isCompletedSwapTablePoint = true;
        this.executorService.shutdownNow();
    }
}
