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

package org.opengauss.datachecker.extract.cache;

import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.Topic;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TopicCache
 *
 * @author ：wangchao
 * @date ：Created in 2023/4/23
 * @since ：11
 */
public class TopicCache {
    private static final Map<String, Topic> TOPIC_CACHE = new ConcurrentHashMap<>();
    private static volatile Endpoint endpoint;

    /**
     * init current endpoint
     *
     * @param currentEndpoint currentEndpoint
     */
    public static void initEndpoint(Endpoint currentEndpoint) {
        endpoint = currentEndpoint;
    }

    /**
     * add topic cache
     *
     * @param topic topic
     */
    public static void add(Topic topic) {
        if (Objects.isNull(topic)) {
            return;
        }
        if (Objects.equals(endpoint, Endpoint.SOURCE)) {
            topic.setTopicName(topic.getSourceTopicName());
        } else {
            topic.setTopicName(topic.getSinkTopicName());
        }
        TOPIC_CACHE.put(topic.getTableName(), topic);
    }

    /**
     * get current table's topic
     *
     * @param table table name
     * @return Topic
     */
    public static Topic getTopic(String table) {
        return Objects.requireNonNull(TOPIC_CACHE.get(table), table + " is not found the topic information");
    }
}
