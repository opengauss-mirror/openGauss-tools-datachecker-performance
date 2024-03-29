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

package org.opengauss.datachecker.common.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TaskUtilTest
 *
 * @author ：wangchao
 * @date ：Created in 2023/4/23
 * @since ：11
 */
class TaskUtilTest {
    @DisplayName("calc task of 0 row")
    @Test
    void testCalcAutoTaskCount_0_row() {
        assertThat(TaskUtil.calcAutoTaskCount(0L)).isEqualTo(1);
    }

    @DisplayName("calc task of 1000 row")
    @Test
    void testCalcAutoTaskCount_1000_row() {
        assertThat(TaskUtil.calcAutoTaskCount(1000L)).isEqualTo(1);
    }

    @DisplayName("calc task of 50000 row")
    @Test
    void testCalcAutoTaskCount_50000_row() {
        assertThat(TaskUtil.calcAutoTaskCount(50000L)).isEqualTo(1);
    }

    @DisplayName("calc task of 60000 row")
    @Test
    void testCalcAutoTaskCount_60000_row() {
        assertThat(TaskUtil.calcAutoTaskCount(60000L)).isEqualTo(1);
    }

    @DisplayName("calc task of 70000 row")
    @Test
    void testCalcAutoTaskCount_70000_row() {
        assertThat(TaskUtil.calcAutoTaskCount(70000L)).isEqualTo(1);
    }

    @DisplayName("calc task of 75000 row")
    @Test
    void testCalcAutoTaskCount_75000_row() {
        assertThat(TaskUtil.calcAutoTaskCount(75000L)).isEqualTo(2);
    }

    @DisplayName("calc task of 80000 row")
    @Test
    void testCalcAutoTaskCount_80000_row() {
        assertThat(TaskUtil.calcAutoTaskCount(80000L)).isEqualTo(2);
    }

    @DisplayName("calc task of 85000 row")
    @Test
    void testCalcAutoTaskCount_85000_row() {
        assertThat(TaskUtil.calcAutoTaskCount(85000L)).isEqualTo(2);
    }

    @DisplayName("calc task of 430060L row")
    @Test
    void testCalcAutoTaskCount_430060L_row() {
        assertThat(TaskUtil.calcAutoTaskCount(430060L)).isEqualTo(9);
    }

    @DisplayName("calc task of 4300000L row")
    @Test
    void testCalcAutoTaskCount_4300000L_row() {
        assertThat(TaskUtil.calcAutoTaskCount(4300000L)).isEqualTo(11);
    }

    @DisplayName("calc task of 43000000L row")
    @Test
    void testCalcAutoTaskCount_43000000L_row() {
        assertThat(TaskUtil.calcAutoTaskCount(43000000L)).isEqualTo(43);
    }

    @DisplayName("calc task of 100000000L row")
    @Test
    void testCalcAutoTaskCount_100000000L_row() {
        assertThat(TaskUtil.calcAutoTaskCount(100000000L)).isEqualTo(100);
    }

    @DisplayName("calc task of 200000000L row")
    @Test
    void testCalcAutoTaskCount_200000000L_row() {
        assertThat(TaskUtil.calcAutoTaskCount(200000000L)).isEqualTo(200);
    }

    @Test
    void testCalcTablePartitionRowCount() {
        assertThat(TaskUtil.calcTablePartitionRowCount(0L, 0)).isEqualTo(0);
    }

    private int[][] constructor(int row, int step, int end) {
        int[][] res = new int[row][2];
        AtomicInteger start = new AtomicInteger(0);
        IntStream.range(0, row).forEach(idx -> {
            res[idx] = new int[] {start.get(), step};
            start.getAndAdd(step);
        });
        if (end > 0) {
            res[row - 1][1] = end;
        }
        return res;
    }
}
