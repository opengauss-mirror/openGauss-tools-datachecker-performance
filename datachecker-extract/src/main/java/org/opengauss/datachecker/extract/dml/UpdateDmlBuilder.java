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

import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.HexUtil;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * UpdateDmlBuilder
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/14
 * @since ：11
 */
public class UpdateDmlBuilder extends DmlBuilder {
    private TableMetadata metadata;
    private Map<String, String> columnsValues;

    public UpdateDmlBuilder(DataBaseType databaseType, boolean ogCompatibility) {
        super(databaseType, ogCompatibility);
    }

    /**
     * build Schema
     *
     * @param schema Schema
     * @return UpdateDmlBuilder
     */
    public UpdateDmlBuilder schema(@NotNull String schema) {
        super.buildSchema(schema);
        return this;
    }

    /**
     * build dataBaseType
     *
     * @param dataBaseType dataBaseType
     * @return UpdateDmlBuilder
     */
    public UpdateDmlBuilder dataBaseType(@NotNull DataBaseType dataBaseType) {
        super.buildDataBaseType(dataBaseType);
        return this;
    }

    public UpdateDmlBuilder isOgCompatibilityB(boolean isOgCompatibilityB) {
        this.isOgCompatibilityB = isOgCompatibilityB;
        return this;
    }

    /**
     * build tableName
     *
     * @param tableName tableName
     * @return UpdateDmlBuilder
     */
    public UpdateDmlBuilder tableName(@NotNull String tableName) {
        super.buildTableName(tableName);
        return this;
    }

    /**
     * build SQL column value statement fragment
     *
     * @param columnsValues Field values
     * @return UpdateDmlBuilder
     */
    public UpdateDmlBuilder columnsValues(@NotNull Map<String, String> columnsValues) {
        this.columnsValues = columnsValues;
        return this;
    }

    public UpdateDmlBuilder metadata(@NotNull TableMetadata metadata) {
        this.metadata = metadata;
        return this;
    }

    public String build() {
        return Fragment.DML_UPDATE.replace(Fragment.SCHEMA, schema).replace(Fragment.TABLE_NAME, tableName)
                                  .replace(Fragment.COLUMNS, buildColumnsValue())
                                  .replace(Fragment.CONDITION, buildConditionCompositePrimary());
    }

    private String buildConditionCompositePrimary() {
        StringBuilder builder = new StringBuilder();
        final List<ColumnsMetaData> primaryMetaDatas = metadata.getPrimaryMetas();
        for (ColumnsMetaData primaryMeta : primaryMetaDatas) {
            builder.append(primaryMeta.getColumnName()).append(Fragment.EQUAL);
            if (isDigital(primaryMeta.getDataType())) {
                builder.append(columnsValues.get(primaryMeta.getColumnName()));
            } else if (BLOB_LIST.contains(primaryMeta.getDataType())) {
                builder.append(convertValue(HexUtil.toHex(columnsValues.get(primaryMeta.getColumnName()))));
            } else if (BINARY.contains(primaryMeta.getDataType())) {
                builder.append(convertValue(HexUtil.HEX_PREFIX + columnsValues.get(primaryMeta.getColumnName())));
            } else {
                builder.append(convertValue(columnsValues.get(primaryMeta.getColumnName())));
            }
            builder.append(Fragment.AND);
        }
        final int length = builder.length();
        builder.delete(length - 4, length);
        return builder.toString();
    }

    private String convertValue(String fieldValue) {
        return Fragment.SINGLE_QUOTES + fieldValue + Fragment.SINGLE_QUOTES;
    }

    private boolean isDigital(String dataType) {
        return DIGITAL.contains(dataType);
    }

    private String buildColumnsValue() {
        StringBuilder builder = new StringBuilder();
        final List<ColumnsMetaData> columnMetaDatas = metadata.getColumnsMetas();
        for (ColumnsMetaData columnMeta : columnMetaDatas) {
            if (Objects.equals(columnMeta.getColumnKey(), ColumnKey.PRI)) {
                continue;
            }
            final String columnName = columnMeta.getColumnName();
            builder.append(columnName).append(Fragment.EQUAL);
            final String columnValue = columnsValues.get(columnName);
            if (isDigital(columnMeta.getDataType())) {
                builder.append(columnValue);
            } else if (BLOB_LIST.contains(columnMeta.getDataType())) {
                builder.append(convertValue(HexUtil.toHex(columnValue)));
            } else if (BINARY.contains(columnMeta.getDataType())) {
                builder.append(convertValue(HexUtil.HEX_PREFIX + columnValue));
            } else {
                builder.append(convertValue(columnValue));
            }
            builder.append(Fragment.COMMA);
        }
        final int length = builder.length();
        builder.delete(length - 3, length);
        return builder.toString();
    }
}
