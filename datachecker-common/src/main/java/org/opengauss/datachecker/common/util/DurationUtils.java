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

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * DurationUtils
 *
 * @author ：wangchao
 * @date ：Created in 2024/3/11
 * @since ：11
 */
public class DurationUtils {
    /**
     * startTime and now diff seconds
     *
     * @param startTime startTime
     * @return seconds
     */
    public static long betweenSeconds(LocalDateTime startTime) {
        return Duration.between(startTime, LocalDateTime.now())
                       .toSeconds();
    }

    /**
     * startTime and now diff seconds
     *
     * @param startTime startTime
     * @param endTime   endTime
     * @return seconds
     */
    public static long betweenSeconds(LocalDateTime startTime, LocalDateTime endTime) {
        return Duration.between(startTime, endTime)
                       .toSeconds();
    }
}
