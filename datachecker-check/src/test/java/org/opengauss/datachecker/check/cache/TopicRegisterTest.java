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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.util.TopicUtil;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TopicRegisterTest
 *
 * @author wang chao
 * @date 2022/5/12 19:17
 * @since 11
 **/
class TopicRegisterTest {
    private TopicRegister topicRegisterUnderTest;

    @BeforeEach
    void setUp() {
        topicRegisterUnderTest = new TopicRegister();
    }

    @DisplayName("register source")
    @Test
    void testRegisterSource() {
        // Setup
        final Topic expectedResult = new Topic();
        expectedResult.setTableName("tableName1");
        expectedResult
            .setSourceTopicName(TopicUtil.buildTopicName("process", Endpoint.SOURCE, expectedResult.getTableName()));
        expectedResult.setPartitions(0);

        // Run the test
        final Topic result = topicRegisterUnderTest.register("tableName1", 0, Endpoint.SOURCE);

        // Verify the results
        assertThat(result).isEqualTo(expectedResult);
    }

    @DisplayName("register sink")
    @Test
    void testRegisterSink() {
        // Setup
        final Topic expectedResult = new Topic();
        expectedResult.setTableName("tableName2");
        expectedResult
            .setSinkTopicName(TopicUtil.buildTopicName("process", Endpoint.SINK, expectedResult.getTableName()));
        expectedResult.setPartitions(0);

        // Run the test
        final Topic result = topicRegisterUnderTest.register("tableName2", 0, Endpoint.SINK);

        // Verify the results
        assertThat(result).isEqualTo(expectedResult);
    }

    @DisplayName("register source and sink")
    @Test
    void testRegisterSourceAndSink() {
        // Setup
        final Topic expectedResult = new Topic();
        expectedResult.setTableName("tableName3");
        expectedResult
            .setSourceTopicName(TopicUtil.buildTopicName("process", Endpoint.SOURCE, expectedResult.getTableName()));
        expectedResult
            .setSinkTopicName(TopicUtil.buildTopicName("process", Endpoint.SINK, expectedResult.getTableName()));
        expectedResult.setPartitions(0);

        // Run the test
        topicRegisterUnderTest.register("tableName3", 0, Endpoint.SOURCE);
        final Topic result = topicRegisterUnderTest.register("tableName3", 0, Endpoint.SINK);

        // Verify the results
        assertThat(result).isEqualTo(expectedResult);
    }
}
