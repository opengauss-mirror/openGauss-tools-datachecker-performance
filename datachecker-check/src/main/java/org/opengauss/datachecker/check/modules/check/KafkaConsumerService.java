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
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.opengauss.datachecker.check.config.KafkaConsumerConfig;
import org.opengauss.datachecker.common.util.IdGenerator;
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
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnBean({KafkaConsumerConfig.class})
public class KafkaConsumerService {
    private static final String CLIENT_ID_SUFFIX = "Random";

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
            consumer = kafkaConsumerConfig.consumerFactory().createConsumer(IdGenerator.nextId36(), CLIENT_ID_SUFFIX);
        } else {
            consumer = kafkaConsumerConfig.consumerFactory().createConsumer();
        }
        return (KafkaConsumer<String, String>) consumer;
    }
}
