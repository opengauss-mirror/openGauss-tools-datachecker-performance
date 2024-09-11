/*
 * Copyright (c) 2024-2024 Huawei Technologies Co.,Ltd.
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

package org.opengauss.datachecker.common.entry.enums;

import lombok.Getter;

import java.util.Arrays;

/**
 * LowerCaseTableNames
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
@Getter
public enum LowerCaseTableNames implements IEnum {
    /**
     * 表名大小写敏感,区分大小写
     */
    SENSITIVE("0", "lower_case_table_names=0"),
    /**
     * 表名大小写不敏感,不区分大小写
     */
    INSENSITIVE("1", "lower_case_table_names=1"),

    /**
     * 未知
     */
    UNKNOWN("unknown", "lower_case_table_names=unknown");

    private final String code;
    private final String description;

    /**
     * constructor
     *
     * @param code        code
     * @param description description
     */
    LowerCaseTableNames(String code, String description) {
        this.code = code;
        this.description = description;
    }

    /**
     * get enum by code
     *
     * @param code code
     * @return enum
     */
    public static LowerCaseTableNames codeOf(String code) {
        return Arrays.stream(LowerCaseTableNames.values())
                .filter(key -> key.code.equals(code)).findFirst().orElse(null);
    }
}
