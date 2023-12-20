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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * HexUtilTest
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/10
 * @since ：11
 */
class HexUtilTest {

    /**
     * test binary to hex char
     */
    @Test
    void testBinaryToHex() {
        assertThat(HexUtil.binaryToHex("0000")).isEqualTo("0");
        assertThat(HexUtil.binaryToHex("0001")).isEqualTo("1");
        assertThat(HexUtil.binaryToHex("0010")).isEqualTo("2");
        assertThat(HexUtil.binaryToHex("0011")).isEqualTo("3");
        assertThat(HexUtil.binaryToHex("0100")).isEqualTo("4");
        assertThat(HexUtil.binaryToHex("0101")).isEqualTo("5");
        assertThat(HexUtil.binaryToHex("0110")).isEqualTo("6");
        assertThat(HexUtil.binaryToHex("0111")).isEqualTo("7");
        assertThat(HexUtil.binaryToHex("1000")).isEqualTo("8");
        assertThat(HexUtil.binaryToHex("1001")).isEqualTo("9");
        assertThat(HexUtil.binaryToHex("1010")).isEqualTo("A");
        assertThat(HexUtil.binaryToHex("1011")).isEqualTo("B");
        assertThat(HexUtil.binaryToHex("1100")).isEqualTo("C");
        assertThat(HexUtil.binaryToHex("1101")).isEqualTo("D");
        assertThat(HexUtil.binaryToHex("1110")).isEqualTo("E");
        assertThat(HexUtil.binaryToHex("1111")).isEqualTo("F");
    }
}
