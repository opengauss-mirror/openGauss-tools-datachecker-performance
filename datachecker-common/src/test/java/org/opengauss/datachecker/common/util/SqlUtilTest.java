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
import org.opengauss.datachecker.common.entry.enums.DataBaseType;

import static org.assertj.core.api.Assertions.assertThat;

class SqlUtilTest {
    @DisplayName("test mysql escape ")
    @Test
    void testEscape() {
        assertThat(SqlUtil.escape("content", DataBaseType.MS)).isEqualTo("`content`");
    }

    @DisplayName("test opengauss escape ")
    @Test
    void testOpenGaussEscape() {
        assertThat(SqlUtil.escape("content", DataBaseType.OG)).isEqualTo("\"content\"");
    }
}
