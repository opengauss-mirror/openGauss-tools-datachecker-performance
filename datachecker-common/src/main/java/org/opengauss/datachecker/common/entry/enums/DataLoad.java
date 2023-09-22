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
 * DataLoad
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/10
 * @since ：11
 */
@Getter
public enum DataLoad implements IEnum {
    /**
     * load data by jdbc
     */
    JDBC("jdbc", "load data by jdbc"),
    /**
     * load data by csv
     */
    CSV("csv", "load data by csv");

    private final String code;
    private final String description;

    DataLoad(String code, String description) {
        this.code = code;
        this.description = description;
    }

    /**
     * DataBaseType api description
     */
    public static final String API_DESCRIPTION = "DataLoadType [JDBC-load data by jdbc,CSV-load data by csv]";
}
