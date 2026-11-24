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

import org.opengauss.datachecker.common.entry.enums.IEnum;

/**
 * EnumUtil
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class EnumUtil {

    /**
     * Returns the elements of this enum class or null if this Class object does not represent an enum type
     *
     * @param clazz clazz
     * @param code  code
     * @param <T>   clazz type
     * @return enum elements
     */
    public static <T extends IEnum> T valueOfIgnoreCase(Class<T> clazz, String code) {
        return valueOf(clazz, code, true);
    }

    /**
     * Returns the elements of this enum class or null if this Class object does not represent an enum type
     *
     * @param clazz    clazz
     * @param code     code
     * @param isIgnore isIgnore
     * @param <T>      clazz type
     * @return enum elements
     */
    public static <T extends IEnum> T valueOf(Class<T> clazz, String code, boolean isIgnore) {
        T[] enums = values(clazz);
        if (enums == null || enums.length == 0) {
            return null;
        }

        for (T t : enums) {
            if (isIgnore && t.getCode().equalsIgnoreCase(code)) {
                return t;
            } else if (t.getCode().equals(code)) {
                return t;
            }
        }
        return null;
    }

    /**
     * Returns the elements of this enum class or null if this Class object does not represent an enum type
     *
     * @param clazz clazz
     * @param code  code
     * @param <T>   clazz type
     * @return enum elements
     */
    public static <T extends IEnum> T valueOf(Class<T> clazz, String code) {
        return valueOf(clazz, code, false);
    }

    /**
     * Returns the elements of this enum class or null if this Class object does not represent an enum type
     *
     * @param clazz clazz
     * @param <T>   clazz type
     * @return enum
     */
    public static <T extends IEnum> T[] values(Class<T> clazz) {
        if (!clazz.isEnum()) {
            throw new IllegalArgumentException("Class[" + clazz + "] is not an enumeration type");
        }
        return clazz.getEnumConstants();
    }
}
