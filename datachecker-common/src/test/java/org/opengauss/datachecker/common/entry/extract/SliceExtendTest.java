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

package org.opengauss.datachecker.common.entry.extract;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SliceExtendTest {

    private SliceExtend sliceExtendUnderTest;

    @BeforeEach
    void setUp() {
        sliceExtendUnderTest = new SliceExtend();
    }

    @Test
    void testToString() {
        SliceExtend sliceExtend = new SliceExtend();
        sliceExtend.setName("slice_name");
        sliceExtend.setNo(1);
        sliceExtend.setTableHash(10023323L);
        sliceExtend.setStartOffset(10000);
        sliceExtend.setEndOffset(20000);
        sliceExtend.setCount(10000);
        assertThat(sliceExtend).toString().equals("{ name=slice_name, no=1 , offset=[10000, 20000] , count=10000 , tableHash=10023323}");
    }
}
