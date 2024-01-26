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

package org.opengauss.datachecker.extract.task;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.util.TestJsonUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.opengauss.datachecker.extract.util.TestJsonUtil.KEY_META_DATA_13_TABLE;

class ExtractTaskBuilderTest {

    private ExtractTaskBuilder extractTaskBuilderUnderTest;

    @BeforeEach
    void setUp() {
        HashMap<String, TableMetadata> result = TestJsonUtil.parseHashMap(KEY_META_DATA_13_TABLE, TableMetadata.class);
        MetaDataCache.putMap(result);
        extractTaskBuilderUnderTest = new ExtractTaskBuilder();
    }

    @DisplayName("build task table empty")
    @Test
    void testBuilder_empty_table_exception() {
        List<ExtractTask> taskList = extractTaskBuilderUnderTest.builder(Set.of());
        assertThat(taskList).isEmpty();
    }

    @DisplayName("build task table")
    @Test
    void testBuilder() {
        String tableName = "t_data_checker_0033_02";
        final List<ExtractTask> taskBuilderResult = extractTaskBuilderUnderTest.builder(Set.of(tableName));
        final ExtractTask resultTask = taskBuilderResult.get(0);
        final ExtractTask expectTask = new ExtractTask();
        expectTask.setTableName(tableName).setTaskName("extract_task_" + tableName).setDivisionsTotalNumber(1)
                  .setDivisionsOrdinal(1).setOffset(10).setStart(0).setTableMetadata(MetaDataCache.get(tableName));
        assertThat(resultTask).isEqualTo(expectTask);
    }
}
