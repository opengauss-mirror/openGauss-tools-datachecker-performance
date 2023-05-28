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

package org.opengauss.datachecker.check.cache;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.util.TopicUtil;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TopicRegister
 *
 * @author ：wangchao
 * @date ：Created in 2023/4/23
 * @since ：11
 */
@Slf4j
@Service
public class TopicRegister {
    private static volatile Map<String, Topic> TOPIC_CACHE = new ConcurrentHashMap<>();

    private volatile String process;

    /**
     * init current check process
     *
     * @param process process
     */
    public void initProcess(String process) {
        this.process = process;
    }

    /**
     * add table's topic
     *
     * @param table           table name
     * @param topicPartitions topicPartitions
     * @param endpoint        endpoint
     * @return topic
     */
    public Topic register(String table, int topicPartitions, Endpoint endpoint) {
        synchronized (table) {
            Topic topic = TOPIC_CACHE.get(table);
            if (Objects.isNull(topic)) {
                topic = new Topic();
                topic.setTableName(table);
                topic.setPartitions(topicPartitions);
                setTopicName(table, topic);
                TOPIC_CACHE.put(table, topic);
            }
            log.debug("register topic {}-{}", endpoint, topic.toString());
        }
        return TOPIC_CACHE.get(table);
    }

    private void setTopicName(String table, Topic topic) {
        topic.setSourceTopicName(TopicUtil.buildTopicName(process, Endpoint.SOURCE, table));
        topic.setSinkTopicName(TopicUtil.buildTopicName(process, Endpoint.SINK, table));
    }

    /**
     * get table's topic
     *
     * @param table table
     * @return topic
     */
    public Topic getTopic(String table) {
        return TOPIC_CACHE.get(table);
    }
}