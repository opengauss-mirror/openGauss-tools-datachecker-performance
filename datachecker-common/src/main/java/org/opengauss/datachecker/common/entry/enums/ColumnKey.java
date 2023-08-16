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

package org.opengauss.datachecker.common.entry.enums;

import lombok.Getter;

/**
 * ColumnKey {@value API_DESCRIPTION }
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
@Getter
public enum ColumnKey implements IEnum {
    /**
     * PRI
     */
    PRI("PRI", "primary key"),
    /**
     * UNI
     */
    UNI("UNI", "union key"),
    /**
     * MUL
     */
    MUL("MUL", "composite primary key");

    private final String code;
    private String description;

    ColumnKey(String code, String description) {
        this.code = code;
        this.description = description;
    }

    /**
     * ColumnKey api description
     */
    public static final String API_DESCRIPTION = "ColumnKey [PRI-PRI,UNI-UNI, MUL-MUL]";
}
