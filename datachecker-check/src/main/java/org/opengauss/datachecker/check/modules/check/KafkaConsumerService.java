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

package org.opengauss.datachecker.check.modules.check;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.config.KafkaConsumerConfig;
import org.opengauss.datachecker.common.util.IdGenerator;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

/**
 * KafkaConsumerService
 *
 * @author ：wangchao
 * @date ：Created in 2022/8/31
 * @since ：11
 */
@Service
@RequiredArgsConstructor
@ConditionalOnBean({KafkaConsumerConfig.class})
public class KafkaConsumerService {
    private static final Logger log = LogUtils.getLogger();

    private final KafkaConsumerConfig kafkaConsumerConfig;

    @Value("${data.check.retry-fetch-record-times}")
    private int retryFetchRecordTimes = 5;

    /**
     * consumer retry times
     *
     * @return consumer retry times
     */
    public int getRetryFetchRecordTimes() {
        return retryFetchRecordTimes;
    }

    /**
     * consumer
     *
     * @param isNewGroup isNewGroup
     * @return consumer
     */
    public KafkaConsumer<String, String> buildKafkaConsumer(boolean isNewGroup) {
        Consumer<String, String> consumer;
        if (isNewGroup) {
            consumer = kafkaConsumerConfig.consumerFactory(IdGenerator.nextId36())
                                          .createConsumer();
        } else {
            consumer = kafkaConsumerConfig.consumerFactory()
                                          .createConsumer();
        }
        return (KafkaConsumer<String, String>) consumer;
    }

    /**
     * 创建一个kafka消费者，并指定对应的GroupID
     *
     * @param group kafka消费者Group ID
     * @return kafka消费者
     */
    public KafkaConsumer<String, String> buildKafkaConsumer(String group) {
        return (KafkaConsumer<String, String>) kafkaConsumerConfig.consumerFactory(group)
                                                                  .createConsumer();
    }
}
