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

package org.opengauss.datachecker.check.event;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author ：wangchao
 * @date ：Created in 2023/3/7
 * @since ：11
 */
@Component
public class DeleteTopicsEventListener implements ApplicationListener<DeleteTopicsEvent> {
    private static final Logger log = LogUtils.getKafkaLogger();
    @Resource
    private CustomEventHistory customEventHistory;
    @Resource
    private FeignClientService feignClient;
    private KafkaAdminClient adminClient = null;
    private final Lock lock = new ReentrantLock();

    @Override
    public void onApplicationEvent(DeleteTopicsEvent event) {
        lock.lock();
        try {
            log.info("delete topic event : {}", event.getMessage());
            final Object source = event.getSource();
            initAdminClient();
            final DeleteTopics deleteOption = (DeleteTopics) source;
            deleteTopic(deleteOption.getTopicList());
            feignClient.notifyCheckTableFinished(Endpoint.SOURCE, deleteOption.getTableName());
            ThreadUtil.sleep(100);
            feignClient.notifyCheckTableFinished(Endpoint.SINK, deleteOption.getTableName());
        } catch (Exception exception) {
            log.error("delete topic has error ", exception);
        } finally {
            lock.unlock();
            customEventHistory.completedEvent(event);
        }
    }

    private void deleteTopic(List<String> deleteTopicList) {
        try {
            log.debug("delete topic [{}] start", deleteTopicList);
            DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(deleteTopicList);
            final KafkaFuture<Void> kafkaFuture = deleteTopicsResult.all();
            kafkaFuture.get();
            log.debug("delete topic [{}] finished", deleteTopicList);
        } catch (InterruptedException | ExecutionException ignore) {
            log.error("delete topic [{}] error :  ", deleteTopicList, ignore);
        }
    }

    private void initAdminClient() {
        if (this.adminClient == null) {
            Map<String, Object> props = new HashMap<>(1);
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigCache.getValue(ConfigConstants.KAFKA_SERVERS));
            this.adminClient = (KafkaAdminClient) KafkaAdminClient.create(props);
            log.info("init admin client [{}]", ConfigCache.getValue(ConfigConstants.KAFKA_SERVERS));
        }
    }

    @PreDestroy
    public void closeAdminClient() {
        if (adminClient != null) {
            try {
                adminClient.close(Duration.ZERO);
                log.info("check kafkaAdminClient close.");
            } catch (Exception e) {
                log.error("check kafkaAdminClient close error: ", e);
            }
        }
    }
}
