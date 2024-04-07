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

package org.opengauss.datachecker.check.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static org.opengauss.datachecker.common.constant.ConfigConstants.KAFKA_SERVERS;
import static org.opengauss.datachecker.common.constant.ConfigConstants.KAFKA_DEFAULT_GROUP_ID;
import static org.opengauss.datachecker.common.constant.ConfigConstants.KAFKA_AUTO_COMMIT;
import static org.opengauss.datachecker.common.constant.ConfigConstants.KAFKA_AUTO_OFFSET_RESET;
import static org.opengauss.datachecker.common.constant.ConfigConstants.KAFKA_MAX_POLL_RECORDS;
import static org.opengauss.datachecker.common.constant.ConfigConstants.KAFKA_REQUEST_TIMEOUT;
import static org.opengauss.datachecker.common.constant.ConfigConstants.KAFKA_FETCH_MAX_BYTES;

/**
 * KafkaConsumerConfig
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/17
 * @since ：11
 */
@Component
public class KafkaConsumerConfig {
    private LinkedBlockingQueue<KafkaConsumer<String, String>> consumerPool = new LinkedBlockingQueue<>();

    /**
     * 初始化消费者池
     */
    public void initConsumerPool() {
        int maxPoolSize = ConfigCache.getIntValue(ConfigConstants.MAXIMUM_POOL_SIZE);
        String process = ConfigCache.getValue(ConfigConstants.PROCESS_NO);
        for (int i = 0; i < maxPoolSize; i++) {
            consumerPool.add((KafkaConsumer<String, String>) consumerFactory(process).createConsumer());
        }
    }

    /**
     * 获取一个空闲Kafka consumer
     *
     * @return consumer
     */
    public KafkaConsumer<String, String> takeConsumer() {
        try {
            return consumerPool.take();
        } catch (InterruptedException e) {
            throw new CheckingException("take consumer interruptedException");
        }
    }

    /**
     * 归还当前kafka consumer 到消费者池
     *
     * @param consumer consumer
     */
    public void returnConsumer(KafkaConsumer<String, String> consumer) {
        consumerPool.add(consumer);
    }

    /**
     * 等待全部消费者归还后，关闭消费者池
     */
    public void closeConsumerPool() {
        int maxPoolSize = ConfigCache.getIntValue(ConfigConstants.MAXIMUM_POOL_SIZE);
        while (consumerPool.size() < maxPoolSize) {
            ThreadUtil.sleepMaxHalfSecond();
        }
        while (!consumerPool.isEmpty()) {
            KafkaConsumer<String, String> consumer = takeConsumer();
            consumer.close();
        }
    }

    /**
     * consumerConfigs
     *
     * @param groupId  groupId
     * @return consumerConfigs
     */
    public Map<String, Object> consumerConfigs(String groupId) {
        Thread.currentThread().setContextClassLoader(null);
        Map<String, Object> propsMap = new HashMap<>(InitialCapacity.CAPACITY_8);
        propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigCache.getValue(KAFKA_SERVERS));
        propsMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ConfigCache.getBooleanValue(KAFKA_AUTO_COMMIT));
        propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propsMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, ConfigCache.getValue(KAFKA_AUTO_OFFSET_RESET));
        propsMap.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, ConfigCache.getIntValue(KAFKA_MAX_POLL_RECORDS));
        propsMap.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, ConfigCache.getIntValue(KAFKA_FETCH_MAX_BYTES));
        propsMap.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, ConfigCache.getIntValue(KAFKA_REQUEST_TIMEOUT));

        if (StringUtils.isEmpty(groupId)) {
            propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, ConfigCache.getValue(KAFKA_DEFAULT_GROUP_ID));
        } else {
            propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
        return propsMap;
    }

    /**
     * consumerFactory
     *
     * @param groupId groupId
     * @return ConsumerFactory
     */
    public ConsumerFactory<String, String> consumerFactory(String groupId) {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs(groupId));
    }

    public ConsumerFactory<String, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfigs(null));
    }
}
