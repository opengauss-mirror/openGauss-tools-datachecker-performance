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

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * MathUtils
 *
 * @author ：wangchao
 * @date ：Created in 2023/4/24
 * @since ：11
 */
public class MathUtils {
    /**
     * Convert to percentage
     */
    public static final int CONVERT_PERCENTAGE = 100;
    private static final int DEF_DIV_SCALE = 2;

    private MathUtils() {
    }

    /**
     * Double  value1 subtract value2
     *
     * @param value1 value1
     * @param value2 value2
     * @return doubleValue
     */
    public static double sub(double value1, double value2) {
        return new BigDecimal(Double.toString(value1)).subtract(new BigDecimal(Double.toString(value2))).doubleValue();
    }

    /**
     * Double data is rounded and converted to a percentage
     *
     * @param doubleValue doubleValue
     * @return doubleValue
     */
    public static double round100(double doubleValue) {
        return mul(doubleValue, CONVERT_PERCENTAGE);
    }

    /**
     * Divide doublevalue1 by doublevalue2 and round the result to 2 decimal places
     *
     * @param doubleValue1 doubleValue
     * @param doubleValue2 doubleValue
     * @return doubleValue
     */
    public static double divRound(double doubleValue1, double doubleValue2) {
        BigDecimal b1 = new BigDecimal(Double.toString(doubleValue1));
        BigDecimal b2 = new BigDecimal(Double.toString(doubleValue2));
        if (b1.compareTo(BigDecimal.ZERO) == 0) {
            return BigDecimal.ZERO.doubleValue();
        }
        return b1.divide(b2, DEF_DIV_SCALE, RoundingMode.HALF_UP).doubleValue();
    }

    /**
     * double value1 * value2
     *
     * @param value1 value1
     * @param value2 value2
     * @return doubleValue
     */
    public static double mul(double value1, double value2) {
        return new BigDecimal(Double.toString(value1)).multiply(new BigDecimal(Double.toString(value2))).doubleValue();
    }
}
