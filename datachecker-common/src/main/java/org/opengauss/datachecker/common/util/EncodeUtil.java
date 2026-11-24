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

import java.util.Arrays;

/**
 * @author ：wangchao
 * @date ：Created in 2023/3/6
 * @since ：11
 */
public class EncodeUtil {

    /**
     * get array data by array index from and to
     *
     * @param chars source char array
     * @param from  from
     * @param to    to
     * @return string
     */
    public static String parse(char[] chars, int from, int to) {
        return String.valueOf(Arrays.copyOfRange(chars, from, to));
    }

    /**
     * get array data by array index from and to and parse to int
     *
     * @param chars source char array
     * @param from  from
     * @param to    to
     * @return string
     */
    public static int parseInt(char[] chars, int from, int to) {
        return Integer.parseInt(parse(chars, from, to));
    }

    /**
     * get array data by array index from and to and parse to long
     *
     * @param chars source char array
     * @param from  from
     * @param to    to
     * @return string
     */
    public static long parseLong(char[] chars, int from, int to) {
        return Long.parseLong(parse(chars, from, to));
    }

    /**
     * format int value %02d
     *
     * @param value value
     * @return string
     */
    public static String format(int value) {
        return String.format("%02d", value);
    }
}
