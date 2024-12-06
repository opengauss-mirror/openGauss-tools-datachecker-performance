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

import cn.hutool.core.bean.BeanUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.common.CheckPointBean;
import org.opengauss.datachecker.common.entry.common.CheckPointData;
import org.opengauss.datachecker.common.entry.common.PointPair;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
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

    private static final int MAX_RETRY_TIMES = 100;
    private static final BlockingQueue<String> CHECK_POINT_QUEUE = new LinkedBlockingQueue<>();
    private static final Logger log = LogUtils.getLogger(CheckPointSwapRegister.class);

    private final Map<String, List<CheckPointBean>> sourceTablePointCache = new ConcurrentHashMap<>();
    private final Map<String, List<CheckPointBean>> sinkTablePointCache = new ConcurrentHashMap<>();
    private final KafkaServiceManager kafkaServiceManager;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ExecutorService checkPointConsumer;
    private final ExecutorService checkPointSender;
    private String checkPointSwapTopicName;
    private boolean isCompletedSwapTablePoint = false;
    private boolean isSinkStop;
    private boolean isSourceStop;
    private int maxSliceSize;
    private volatile String table = null;

    public CheckPointSwapRegister(KafkaServiceManager kafkaServiceManager, String checkPointTopic) {
        this.checkPointSwapTopicName = checkPointTopic;
        this.kafkaServiceManager = kafkaServiceManager;
        this.kafkaTemplate = kafkaServiceManager.getKafkaTemplate();
        this.checkPointConsumer = ThreadUtil.newSingleThreadExecutor();
        this.checkPointSender = ThreadUtil.newSingleThreadExecutor();
        this.maxSliceSize = ConfigCache.getIntValue(ConfigConstants.MAXIMUM_TABLE_SLICE_SIZE);
    }

    public void stopMonitor(Endpoint endpoint) {
        if (Objects.equals(endpoint, Endpoint.SOURCE)) {
            LogUtils.info(log, " {} finished checkpoint", endpoint);
            this.isSourceStop = true;
        }
        if (Objects.equals(endpoint, Endpoint.SINK)) {
            LogUtils.info(log, " {} finished checkpoint", endpoint);
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
            CheckPointData calculateCheckPoint = null;
            List<PointPair> sourcePoints;
            List<PointPair> sinkPoints;
            while (!isCompletedSwapTablePoint) {
                synchronized (this) {
                    try {
                        table = CHECK_POINT_QUEUE.poll();
                        if ((Objects.isNull(table) || StringUtils.isEmpty(table)) && !isCompletedSwapTablePoint) {
                            continue;
                        }
                        LogUtils.info(log, "start calculate checkpoint [{}][{}]", table, CHECK_POINT_QUEUE.size());
                        if (table != null && sourcePointCounter.containsKey(table) && sinkPointCounter.containsKey(
                            table)) {
                            sourcePoints = sourcePointCounter.get(table).getCheckPointList();
                            sinkPoints = sinkPointCounter.get(table).getCheckPointList();
                            calculateCheckPoint = calculateCheckPoint(table, isCheckPointDigit(table), sourcePoints,
                                sinkPoints);
                            calculateCheckPoint.setEndpoint(Endpoint.CHECK);
                            final CheckPointData sendCheckPointData = calculateCheckPoint;
                            sendCheckPointData.getCheckPointList().forEach(point -> {
                                CheckPointBean tmpBean = new CheckPointBean();
                                BeanUtil.copyProperties(sendCheckPointData, tmpBean);
                                tmpBean.setCheckPoint(point);
                                sendMsg(checkPointSwapTopicName, Endpoint.CHECK.getDescription(), tmpBean, 0);
                            });
                            log.info("send checkpoint data to extract, table: {} size:{}",
                                sendCheckPointData.getTableName(), sendCheckPointData.getCheckPointList().size());
                        }
                    } catch (Exception ex) {
                        log.error("checkPointSender error {}", table, ex);
                    }
                }
            }
        });
    }

    private void sendMsg(String topic, String key, CheckPointBean tmpBean, int reTryTimes) {
        try {
            kafkaTemplate.send(topic, key, JSONObject.toJSONString(tmpBean));
        } catch (TimeoutException ex) {
            if (reTryTimes > MAX_RETRY_TIMES) {
                log.error("send msg to kafka timeout, topic: {} key: {} reTryTimes: {}", topic, key, reTryTimes);
            }
            ThreadUtil.sleepLongCircle(++reTryTimes);
            sendMsg(topic, key, tmpBean, reTryTimes);
        }
    }

    private boolean isCheckPointDigit(String table) {
        return sourcePointCounter.get(table).isDigit();
    }

    /**
     * poll checkpoint data from kafka
     */
    public void pollSwapPoint() {
        checkPointConsumer.submit(() -> {
            KafkaConsumer<String, String> kafkaConsumer = kafkaServiceManager.getKafkaConsumer(true);
            trySubscribe(kafkaConsumer);
            ConsumerRecords<String, String> records;
            while (!isCompletedSwapTablePoint) {
                try {
                    records = kafkaConsumer.poll(Duration.ofSeconds(1));
                    processRecords(records);
                    process(sourceTablePointCache, sourcePointCounter,
                        "add source checkpoint data to queue, table: {} size:{}");
                    process(sinkTablePointCache, sinkPointCounter,
                        "add sink checkpoint data to queue, table: {} size:{}");
                } catch (Exception ex) {
                    if (Objects.equals("java.lang.InterruptedException", ex.getMessage())) {
                        LogUtils.warn(log, "kafka consumer stop by Interrupted");
                    } else {
                        LogUtils.error(log, "pollSwapPoint ", ex);
                    }
                }
            }
            LogUtils.warn(log, "close check point swap consumer {} :{}", checkPointSwapTopicName,
                kafkaConsumer.groupMetadata().groupId());
            kafkaServiceManager.closeConsumer(kafkaConsumer);
        });
    }

    private void processRecords(ConsumerRecords<String, String> records) {
        if (!records.isEmpty()) {
            records.forEach(record -> {
                CheckPointBean pointBean = JSONObject.parseObject(record.value(), CheckPointBean.class);
                if (Objects.equals(record.key(), Endpoint.SOURCE.getDescription())) {
                    if (sourceTablePointCache.containsKey(pointBean.getTableName())) {
                        sourceTablePointCache.get(pointBean.getTableName()).add(pointBean);
                    } else {
                        List<CheckPointBean> list = new LinkedList<>();
                        list.add(pointBean);
                        sourceTablePointCache.put(pointBean.getTableName(), list);
                    }
                } else if (Objects.equals(record.key(), Endpoint.SINK.getDescription())) {
                    if (sinkTablePointCache.containsKey(pointBean.getTableName())) {
                        sinkTablePointCache.get(pointBean.getTableName()).add(pointBean);
                    } else {
                        List<CheckPointBean> list = new LinkedList<>();
                        list.add(pointBean);
                        sinkTablePointCache.put(pointBean.getTableName(), list);
                    }
                } else {
                    log.trace("process ignored");
                }
            });
        }
    }

    private void process(Map<String, List<CheckPointBean>> cache, Map<String, CheckPointData> pointCounter,
        String message) {
        Iterator<Map.Entry<String, List<CheckPointBean>>> iterator = cache.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, List<CheckPointBean>> next = iterator.next();
            List<CheckPointBean> value = next.getValue();
            CheckPointBean checkPointBean = value.get(0);
            CheckPointData checkPointData = new CheckPointData();
            BeanUtil.copyProperties(checkPointBean, checkPointData);
            if (checkPointBean.getSize() == 0) {
                checkPointData.setCheckPointList(List.of());
                pointCounter.put(next.getKey(), checkPointData);
                tryAddCheckQueue(next.getKey());
                iterator.remove();
            } else if (checkPointBean.getSize() == value.size()) {
                checkPointData.setCheckPointList(
                    value.stream().map(CheckPointBean::getCheckPoint).collect(Collectors.toList()));
                checkPointData.setSize(value.size());
                pointCounter.put(next.getKey(), checkPointData);
                iterator.remove();
                tryAddCheckQueue(next.getKey());
                log.info(message, next.getKey(), value.size());
            } else {
                log.trace("process ignored");
            }
        }
    }

    private void tryAddCheckQueue(String tableName) {
        if (sourcePointCounter.containsKey(tableName) && sinkPointCounter.containsKey(tableName)) {
            try {
                CheckPointData sourcePoint = sourcePointCounter.get(tableName);
                CheckPointData sinkPoint = sinkPointCounter.get(tableName);
                if (!validateCheckPointColumn(sourcePoint, sinkPoint)) {
                    if (sourcePoint.getSize() > sinkPoint.getSize()) {
                        sourcePointCounter.put(tableName, sinkPoint);
                    } else {
                        sinkPointCounter.put(tableName, sourcePoint);
                    }
                }
                CHECK_POINT_QUEUE.put(tableName);
            } catch (InterruptedException e) {
                LogUtils.warn(log, "tryAddCheckQueue occur  InterruptedException ");
            }
        }
    }

    private boolean validateCheckPointColumn(CheckPointData sourcePoint, CheckPointData sinkPoint) {
        return StringUtils.equals(sourcePoint.getColName(), sinkPoint.getColName());
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

    private CheckPointData calculateCheckPoint(String tableName, boolean isDigit, List<PointPair> checkPointList,
        List<PointPair> pointList) {
        List<PointPair> unionCheckPoint = new ArrayList<>() {{
            addAll(checkPointList);
            addAll(pointList);
        }};
        List<PointPair> calculatedCheckpoint = listDistinctAndToSorted(isDigit, unionCheckPoint);
        if (!calculatedCheckpoint.isEmpty()) {
            checkPointList.add(calculatedCheckpoint.get(0));
            checkPointList.add(calculatedCheckpoint.get(calculatedCheckpoint.size() - 1));
        }
        List<PointPair> lstDistinctOrdered = listDistinctAndToSorted(isDigit, checkPointList);
        groupListDistinctOrdered(lstDistinctOrdered);
        CheckPointData checkPointData = new CheckPointData();
        CheckPointData result = checkPointData.setTableName(tableName)
            .setDigit(isDigit)
            .setCheckPointList(lstDistinctOrdered);
        result.setSize(result.getCheckPointList().size());
        return result;
    }

    private void groupListDistinctOrdered(List<PointPair> lstDistinctOrdered) {
        List<PointPair> inPointPairs = new ArrayList<>();
        int index = 0;
        for (PointPair point : lstDistinctOrdered) {
            if (canAddPointPairByRowCount(inPointPairs, point, maxSliceSize)) {
                point.setSliceIdx(index);
                inPointPairs.add(point);
            } else {
                index++;
                point.setSliceIdx(index);
                inPointPairs.clear();
                inPointPairs.add(point);
            }
        }
        inPointPairs.clear();
    }

    private boolean canAddPointPairByRowCount(List<PointPair> inPointPairs, PointPair point, int maxSliceSize) {
        // 如果沒有检查点，则直接返回true，添加
        if (inPointPairs.isEmpty()) {
            return true;
        }
        // 如果检查点数量大于等于50，则直接返回false，不添加
        if (inPointPairs.size() >= 50) {
            return false;
        }
        // 如果检查点数量小于50，则计算总行数，如果总行数小于等于maxSliceSize，则直接返回true，添加
        // 总行数 = 当前集合中所有检查点的行数 + 新增检查点的行数
        long count = inPointPairs.stream().mapToLong(PointPair::getRowCount).sum();
        return (count + point.getRowCount()) <= maxSliceSize;
    }

    private List<PointPair> listDistinctAndToSorted(boolean isDigit, List<PointPair> checkPointList) {
        return checkPointList.stream().distinct().sorted(checkPointComparator(isDigit)).collect(Collectors.toList());
    }

    private Comparator<PointPair> checkPointComparator(boolean isDigit) {
        if (isDigit) {
            return Comparator.comparingLong(o -> Long.parseLong((String) o.getCheckPoint()));
        } else {
            return Comparator.comparing(o -> (String) o.getCheckPoint());
        }
    }
}
