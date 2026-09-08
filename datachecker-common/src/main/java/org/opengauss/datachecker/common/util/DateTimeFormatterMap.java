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

import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <pre>
 * DateTimeFormatterMap
 * DataTime  "yyyy-MM-dd HH:mm:ss"
 * "yyyy-MM-dd HH:mm:ss.S"
 * "yyyy-MM-dd HH:mm:ss.SS"
 * "yyyy-MM-dd HH:mm:ss.SSS"
 * "yyyy-MM-dd HH:mm:ss.SSSS"
 * "yyyy-MM-dd HH:mm:ss.SSSSS"
 * "yyyy-MM-dd HH:mm:ss.SSSSSS"
 * </pre>
 *
 * @author ：wangchao
 * @date ：Created in 2024/2/21
 * @since ：11
 */
public class DateTimeFormatterMap {
    private static final Map<Integer, DateTimeFormatter> FORMATTER = new ConcurrentHashMap<>();
    private static final String FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final String FORMAT_S = "S";

    /**
     * Max length of the nanosecond fractional digits (TIMESTAMP scale limit).
     */
    private static final int MAX_NANO_PRECISION = 9;

    static {
        // Pre-build all common precisions 0~9 to avoid runtime computeIfAbsent races among threads
        for (int i = 0; i <= MAX_NANO_PRECISION; i++) {
            FORMATTER.put(i, DateTimeFormatter.ofPattern(patternOf(i)));
        }
    }

    private static String patternOf(int numberOfTimes) {
        return numberOfTimes == 0 ? FORMAT : FORMAT + "." + FORMAT_S.repeat(numberOfTimes);
    }

    /**
     * get DateTimeFormatter by nano second length
     *
     * @param length format nano second length
     * @return DateTimeFormatter
     */
    public DateTimeFormatter get(Integer length) {
        return FORMATTER.computeIfAbsent(length, len -> DateTimeFormatter.ofPattern(patternOf(len)));
    }
}
