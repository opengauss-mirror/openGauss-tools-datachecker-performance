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

package org.opengauss.datachecker.common.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.enums.Endpoint;

import static org.assertj.core.api.Assertions.assertThat;

class TopicUtilTest {

    @DisplayName("build topic table lower and upper")
    @Test
    void testBuildTopicName() {
        assertThat(TopicUtil.buildTopicName("process", Endpoint.SOURCE, "tableName"))
            .isEqualTo("CHECK_process_1_tableName_8");
    }

    @DisplayName("build topic table lower")
    @Test
    void testBuildTopicName2() {
        assertThat(TopicUtil.buildTopicName("process", Endpoint.SOURCE, "table_name"))
            .isEqualTo("CHECK_process_1_table_name_0");
    }

    @DisplayName("build topic table upper")
    @Test
    void testBuildTopicName3() {
        assertThat(TopicUtil.buildTopicName("process", Endpoint.SOURCE, "TABLE_NAME"))
            .isEqualTo("CHECK_process_1_TABLE_NAME_1ff");
    }

    @DisplayName("calc partitions")
    @Test
    void testCalcPartitions() {
        assertThat(TopicUtil.calcPartitions(1)).isEqualTo(1);
    }

    @DisplayName("test formatTableName")
    @Test
    void testFormatTableName() {
        assertThat(TopicUtil.formatTableName("Asdddfs@##$")).isEqualTo("Asdddfs64353536");
        assertThat(TopicUtil.formatTableName("Asdddfs_-#$")).isEqualTo("Asdddfs_-3536");
    }
}
