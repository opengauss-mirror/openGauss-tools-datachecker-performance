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

package org.opengauss.datachecker.extract.dml;

import jakarta.validation.constraints.NotNull;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/14
 * @since ：11
 */
public class InsertDmlBuilder extends DmlBuilder {
    public InsertDmlBuilder(DataBaseType databaseType, boolean ogCompatibility) {
        super(databaseType, ogCompatibility);
    }

    /**
     * build Schema
     *
     * @param schema Schema
     * @return InsertDMLBuilder
     */
    public InsertDmlBuilder schema(@NotNull String schema) {
        super.buildSchema(schema);
        return this;
    }

    /**
     * build tableName
     *
     * @param tableName tableName
     * @return InsertDMLBuilder
     */
    public InsertDmlBuilder tableName(@NotNull String tableName) {
        super.buildTableName(tableName);
        return this;
    }

    /**
     * build sql column statement fragment
     *
     * @param columnsMetas Field Metadata
     * @return InsertDMLBuilder
     */
    public InsertDmlBuilder columns(@NotNull List<ColumnsMetaData> columnsMetas) {
        columns = columnsMetas.stream().map(ColumnsMetaData::getColumnName).collect(Collectors.joining(DELIMITER));
        return this;
    }

    /**
     * build sql column value statement fragment
     *
     * @param columnsMetaList Field Metadata
     * @return InsertDMLBuilder
     */
    public InsertDmlBuilder columnsValue(@NotNull Map<String, String> columnsValue,
        @NotNull List<ColumnsMetaData> columnsMetaList) {
        List<String> valueList = new ArrayList<>(columnsValueList(columnsValue, columnsMetaList));
        this.columnsValue = String.join(DELIMITER, valueList);
        return this;
    }

    public String build() {
        return Fragment.DML_INSERT.replace(Fragment.SCHEMA, schema).replace(Fragment.TABLE_NAME, tableName)
                                  .replace(Fragment.COLUMNS, columns).replace(Fragment.VALUE, columnsValue);
    }

}
