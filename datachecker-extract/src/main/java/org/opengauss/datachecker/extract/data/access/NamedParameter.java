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

package org.opengauss.datachecker.extract.data.access;

import java.util.HashMap;
import java.util.Map;

/**
 * NamedParameter
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/10
 * @since ：11
 */
public class NamedParameter {
    public static final String TABLE_NAME = "tableName";
    public static final String DATABASE_SCHEMA = "databaseSchema";
    public static final String TABLE_COLUMN_NAME = "tableColumn";

    private Map<String, Object> param = new HashMap<>();

    public NamedParameter() {
        this(null, null, null);
    }

    public NamedParameter(Object schemaValue) {
        this(schemaValue, null, null);
    }

    public NamedParameter(Object schemaValue, Object tableValue) {
        this(schemaValue, tableValue, null);
    }

    public NamedParameter(Object schemaValue, Object tableValue, Object columnValue) {
        if (schemaValue != null) {
            putSchema(schemaValue);
        }
        if (tableValue != null) {
            putTableName(tableValue);
        }
        if (columnValue != null) {
            putTableColumn(columnValue);
        }
    }

    public NamedParameter put(String key, Object value) {
        param.put(key, value);
        return this;
    }

    public NamedParameter putSchema(Object value) {
        param.put(DATABASE_SCHEMA, value);
        return this;
    }

    public NamedParameter putTableName(Object value) {
        param.put(TABLE_NAME, value);
        return this;
    }

    public NamedParameter putTableColumn(Object value) {
        param.put(TABLE_COLUMN_NAME, value);
        return this;
    }

    public Map<String, Object> getNamedParam() {
        return param;
    }
}
