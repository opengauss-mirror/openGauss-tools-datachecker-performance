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

import org.opengauss.datachecker.common.entry.enums.DataBaseType;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * SqlUtil
 *
 * @author ：wangchao
 * @date ：Created in 2022/10/31
 * @since ：11
 */
public class SqlUtil {
    private static final String MYSQL_ESCAPE = "`";
    private static final String OPENGAUSS_ESCAPE = "\"";
    private static final String OPENGAUSS_B_ESCAPE = "`";
    private static final Map<DataBaseType, Function<String, String>> ESCAPE = new HashMap<>();
    private static final Map<DataBaseType, Function<String, String>> ESCAPEB = new HashMap<>();

    static {
        ESCAPE.put(DataBaseType.MS, (key) -> MYSQL_ESCAPE + key + MYSQL_ESCAPE);
        ESCAPE.put(DataBaseType.OG, (key) -> OPENGAUSS_ESCAPE + key + OPENGAUSS_ESCAPE);
        ESCAPEB.put(DataBaseType.OG, (key) -> OPENGAUSS_B_ESCAPE + key + OPENGAUSS_B_ESCAPE);
    }

    public static String escape(String content, DataBaseType dataBaseType) {
        if (ESCAPE.containsKey(dataBaseType)) {
            return ESCAPE.get(dataBaseType).apply(content);
        }
        return content;
    }

    public static String escape(String content, DataBaseType dataBaseType, boolean isOgCompatibilityB) {
        if (isOgCompatibilityB) {
            return ESCAPEB.get(dataBaseType).apply(content);
        } else if (ESCAPE.containsKey(dataBaseType)) {
            return ESCAPE.get(dataBaseType).apply(content);
        }
        return content;
    }
}
