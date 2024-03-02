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

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.TopicUtil;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TopicRegister
 *
 * @author ：wangchao
 * @date ：Created in 2023/4/23
 * @since ：11
 */
@Service
public class TopicRegister {
    private static final Logger log = LogUtils.getLogger();
    private static volatile Map<String, Topic> TOPIC_CACHE = new ConcurrentHashMap<>();

    private final Lock lock = new ReentrantLock();

    /**
     * add table's topic
     *
     * @param table    table name
     * @param ptnNum   ptnNum
     * @param endpoint endpoint
     * @return topic
     */
    public Topic register(String table, int ptnNum, Endpoint endpoint) {
        lock.lock();
        try {
            Topic topic = TOPIC_CACHE.get(table);
            if (Objects.isNull(topic)) {
                topic = new Topic();
                topic.setTableName(table);
                topic.setPtnNum(ptnNum);
                setTopicName(table, topic);
                TOPIC_CACHE.put(table, topic);
            }
            log.debug("register topic {}-{}", endpoint, topic.toString());
            return TOPIC_CACHE.get(table);
        } finally {
            lock.unlock();
        }
    }

    private void setTopicName(String table, Topic topic) {
        String process = ConfigCache.getValue(ConfigConstants.PROCESS_NO);
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