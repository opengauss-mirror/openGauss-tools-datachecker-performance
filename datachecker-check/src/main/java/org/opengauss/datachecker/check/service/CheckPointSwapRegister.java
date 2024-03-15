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

package org.opengauss.datachecker.check.service;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.common.CheckPointData;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * CheckPointRegister
 *
 * @author ：lvlintao
 * @date ：Created in 2023/10/28
 * @since ：11
 */
public class CheckPointSwapRegister {
    protected static final Map<String, CheckPointData> sourcePointCounter = new ConcurrentHashMap<>();
    protected static final Map<String, CheckPointData> sinkPointCounter = new ConcurrentHashMap<>();

    private static final BlockingQueue<String> CHECK_POINT_QUEUE = new LinkedBlockingQueue<>();
    private static final Logger log = LogUtils.getLogger(CheckPointSwapRegister.class);

    private final KafkaServiceManager kafkaServiceManager;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ExecutorService checkPointConsumer;
    private final ExecutorService checkPointSender;
    private String checkPointSwapTopicName;
    private boolean isCompletedSwapTablePoint = false;
    private boolean isSinkStop;
    private boolean isSourceStop;

    public CheckPointSwapRegister(KafkaServiceManager kafkaServiceManager, String checkPointTopic) {
        this.checkPointSwapTopicName = checkPointTopic;
        this.kafkaServiceManager = kafkaServiceManager;
        this.kafkaTemplate = kafkaServiceManager.getKafkaTemplate();
        this.checkPointConsumer = ThreadUtil.newSingleThreadExecutor();
        this.checkPointSender = ThreadUtil.newSingleThreadExecutor();
    }

    public void stopMonitor(Endpoint endpoint) {
        if (Objects.equals(endpoint, Endpoint.SOURCE)) {
            this.isSourceStop = true;
        }
        if (Objects.equals(endpoint, Endpoint.SINK)) {
            this.isSinkStop = true;
        }
        if (isSourceStop && isSinkStop) {
            this.isCompletedSwapTablePoint = true;
            this.checkPointConsumer.shutdownNow();
            this.checkPointSender.shutdownNow();
        }
    }

    public void registerCheckPoint() {
        checkPointSender.submit(() -> {
            int deliveredCount = 0;
            String table = null;
            CheckPointData calculateCheckPoint = null;
            List<Object> sourcePoints;
            List<Object> sinkPoints;
            while (!isCompletedSwapTablePoint) {
                try {
                    table = CHECK_POINT_QUEUE.poll();
                    if (StringUtils.isEmpty(table) && !isCompletedSwapTablePoint) {
                        ThreadUtil.sleepHalfSecond();
                        continue;
                    }
                    LogUtils.info(log, "start calculate checkpoint [{}]", table);
                    if (sourcePointCounter.containsKey(table) && sinkPointCounter.containsKey(table)) {
                        sourcePoints = sourcePointCounter.get(table)
                                                         .getCheckPointList();
                        sinkPoints = sinkPointCounter.get(table)
                                                     .getCheckPointList();
                        calculateCheckPoint =
                            calculateCheckPoint(table, isCheckPointDigit(table), sourcePoints, sinkPoints);
                        calculateCheckPoint.setEndpoint(Endpoint.CHECK);
                        kafkaTemplate.send(checkPointSwapTopicName, Endpoint.CHECK.getDescription(),
                            JSONObject.toJSONString(calculateCheckPoint));
                        deliveredCount++;
                        LogUtils.info(log,
                            "send summarized checkpoint topic[{}]:table[{}]:deliverNum[{}]:checkpoint_size[{}]",
                            checkPointSwapTopicName, calculateCheckPoint.getTableName(), deliveredCount,
                            calculateCheckPoint.getCheckPointList()
                                               .size());
                    }
                } catch (Exception ex) {
                    log.error("checkPointSender error {}", table, ex);
                }
            }
        });
    }

    private boolean isCheckPointDigit(String table) {
        return sourcePointCounter.get(table)
                                 .isDigit();
    }

    public void pollSwapPoint() {
        checkPointConsumer.submit(() -> {
            KafkaConsumer<String, String> kafkaConsumer = kafkaServiceManager.getKafkaConsumer(true);
            trySubscribe(kafkaConsumer);
            ConsumerRecords<String, String> records;
            while (!isCompletedSwapTablePoint) {
                try {
                    records = kafkaConsumer.poll(Duration.ofSeconds(1));
                    if (!records.isEmpty()) {
                        records.forEach(record -> {
                            CheckPointData pointData = JSONObject.parseObject(record.value(), CheckPointData.class);
                            String tableName = pointData.getTableName();
                            if (Objects.equals(record.key(), Endpoint.SOURCE.getDescription())) {
                                sourcePointCounter.put(tableName, pointData);
                                tryAddCheckQueue(tableName);
                            } else if (Objects.equals(record.key(), Endpoint.SINK.getDescription())) {
                                sinkPointCounter.put(tableName, pointData);
                                tryAddCheckQueue(tableName);
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
            kafkaServiceManager.closeConsumer(kafkaConsumer);
        });
    }

    private void tryAddCheckQueue(String tableName) {
        if (sourcePointCounter.containsKey(tableName) && sinkPointCounter.containsKey(tableName)) {
            try {
                CHECK_POINT_QUEUE.put(tableName);
            } catch (InterruptedException e) {
                LogUtils.warn(log, "tryAddCheckQueue occur  InterruptedException ");
            }
        }
    }

    private void trySubscribe(KafkaConsumer<String, String> kafkaConsumer) {
        int subscribeTimes = 1;
        boolean isSubscribe = false;
        while (!isSubscribe && subscribeTimes < 5) {
            isSubscribe = subscribe(kafkaConsumer);
            subscribeTimes++;
        }
    }

    private boolean subscribe(KafkaConsumer<String, String> kafkaConsumer) {
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

    private CheckPointData calculateCheckPoint(String tableName, boolean digit, List<Object> checkPointList,
        List<Object> pointList) {
        CheckPointData checkPointData = new CheckPointData();
        List<Object> unionCheckPoint = new ArrayList<>() {{
            addAll(checkPointList);
            addAll(pointList);
        }};
        List<Object> calculatedCheckpoint = listDistinctAndToSorted(digit, unionCheckPoint);

        if (!calculatedCheckpoint.isEmpty()) {
            checkPointList.add(calculatedCheckpoint.get(0));
            checkPointList.add(calculatedCheckpoint.get(calculatedCheckpoint.size() - 1));
        }

        return checkPointData.setTableName(tableName)
                             .setDigit(digit)
                             .setCheckPointList(listDistinctAndToSorted(digit, checkPointList));
    }

    private List<Object> listDistinctAndToSorted(boolean digit, List<Object> checkPointList) {
        return checkPointList.stream()
                             .distinct()
                             .sorted(checkPointComparator(digit))
                             .collect(Collectors.toList());
    }

    private Comparator<Object> checkPointComparator(boolean digit) {
        if (digit) {
            return Comparator.comparingLong(o -> Long.parseLong((String) o));
        } else {
            return Comparator.comparing(o -> (String) o);
        }
    }
}
