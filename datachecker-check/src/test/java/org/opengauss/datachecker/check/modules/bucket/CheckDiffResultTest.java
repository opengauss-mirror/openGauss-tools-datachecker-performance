/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-tools-datachecker-performance is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.check.modules.bucket;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.check.modules.check.AbstractCheckDiffResultBuilder;
import org.opengauss.datachecker.check.modules.check.CheckDiffResult;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.util.JsonObjectUtil;

import java.util.HashSet;
import java.util.Set;

/**
 * CheckDiffResultTest
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/2
 * @since ：11
 */
@ExtendWith(MockitoExtension.class)
public class CheckDiffResultTest {
    private Topic topic;
    private String tableName;
    private String sinkSchema;
    private int partitions;
    private int rowCount;
    private Set<String> updateSet;
    private Set<String> insertSet;
    private Set<String> deleteSet;

    @BeforeEach
    void setUp() {
        topic = new Topic().setTopicName("topic_t_check_test");
        tableName = "t_check_test";
        sinkSchema = "test";
        partitions = 0;
        rowCount = 10000;
        updateSet = new HashSet<>();
        insertSet = new HashSet<>();
        deleteSet = new HashSet<>();
    }

    /**
     * testBuilder
     */
    @DisplayName("openGauss no divisions single primary select SQL build")
    @Test
    void testSelectNoDivisionsSqlBuilder() {
        CheckDiffResult result =
                AbstractCheckDiffResultBuilder.builder().table(tableName).topic(topic.getTopicName())
                        .schema(sinkSchema).partitions(partitions).isTableStructureEquals(true)
                        .isExistTableMiss(false, null)
                        .rowCount(rowCount).errorRate(20)
                        .keyUpdateSet(updateSet)
                        .keyInsertSet(insertSet)
                        .keyDeleteSet(deleteSet).build();
        System.out.println(JsonObjectUtil.format(result));
    }
}
