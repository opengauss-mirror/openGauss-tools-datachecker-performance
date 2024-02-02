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

package org.opengauss.datachecker.common.entry.extract;

import lombok.Data;
import lombok.experimental.Accessors;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.Endpoint;

import java.util.Objects;

/**
 * Database
 *
 * @author ：wangchao
 * @date ：Created in 2022/10/31
 * @since ：11
 */
@Data
@Accessors(chain = true)
public class Database {
    private static final String CSV_SCHEMA_START = "_";
    private static final String CSV_SCHEMA_END = "_tmp";

    String schema;
    DataBaseType databaseType;
    Endpoint endpoint;

    /**
     * get schema
     *
     * @return schema
     */
    public String getSchema() {
        if (Objects.isNull(schema)) {
            return schema;
        }
        if (schema.startsWith(CSV_SCHEMA_START) && schema.endsWith(CSV_SCHEMA_END)) {
            return schema.replaceFirst(CSV_SCHEMA_START, "")
                         .substring(0, schema.length() - 5);
        }
        return schema;
    }
}
