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

package org.opengauss.datachecker.extract.util;

import org.apache.commons.collections4.CollectionUtils;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * MetaDataUtil
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/15
 * @since ：11
 */
public class MetaDataUtil {
    private static final List<String> numberDataTypes =
            List.of("integer", "int", "uint1", "uint2", "uint4", "uint8", "long", "decimal", "numeric",
                    "smallint", "NUMBER", "tinyint", "mediumint", "bigint");
    private static final List<String> dataTypes =
            List.of("integer", "int", "uint1", "uint2", "uint4", "uint8", "long", "decimal", "numeric",
                    "NUMBER", "VARCHAR2", "smallint", "tinyint", "mediumint", "bigint", "character", "char", "varchar",
                    "character varying", "CHAR", "time without time zone", "\"varbinary\"", "varbinary", "time");

    private static final List<String> digitalDataTypes =
            List.of("integer", "int", "uint1", "uint2", "uint4", "uint8", "long", "decimal", "numeric", "smallint",
                    "number", "tinyint", "mediumint", "bigint", "double", "float");

    private static final List<String> LARGE_DIGITAL_TYPES =
            List.of("uint8", "long", "decimal", "numeric", "number", "bigint", "double", "float");

    /**
     * Currently only Oracle LONG / LONG RAW are enabled: they are Oracle legacy streaming
     * types that JDBC requires to be read before the other columns of the row, otherwise the
     * driver drops the LONG stream and later reads fail with "Stream has already been
     * closed" (guaranteed on wide tables). diff_detail queries exclude them entirely to
     * avoid that error; other large types stay unfiltered so diff_detail can still show the
     * real differences of large fields.
     * Note: dataType comes from ADM_TAB_COLUMNS.data_type; LONG RAW is the spaced "LONG RAW".
     */
    private static final List<String> LARGE_COLUMN_TYPES = List.of("long", "long raw");

    /**
     * getTableColumns
     *
     * @param tableMetadata tableMetadata
     * @return table Columns
     */
    public static List<String> getTableColumns(TableMetadata tableMetadata) {
        if (Objects.isNull(tableMetadata)) {
            return emptyList();
        }
        List<ColumnsMetaData> columnsMetas = tableMetadata.getColumnsMetas();
        return getTableColumns(columnsMetas);
    }

    /**
     * getTablePrimaryColumns
     *
     * @param tableMetadata tableMetadata
     * @return table Columns
     */
    public static List<String> getTablePrimaryColumns(TableMetadata tableMetadata) {
        if (Objects.isNull(tableMetadata)) {
            return emptyList();
        }
        List<ColumnsMetaData> primaryMetas = tableMetadata.getPrimaryMetas();
        return getTableColumns(primaryMetas);
    }

    private static ArrayList<String> emptyList() {
        return new ArrayList<>(0);
    }

    private static List<String> getTableColumns(List<ColumnsMetaData> columnsMetas) {
        if (Objects.isNull(columnsMetas)) {
            return emptyList();
        }
        return columnsMetas.stream()
                .sorted(Comparator.comparing(ColumnsMetaData::getOrdinalPosition))
                .map(ColumnsMetaData::getColumnName)
                .collect(Collectors.toUnmodifiableList());
    }

    /**
     * hasNoPrimary
     *
     * @param tableMetadata tableMetadata
     * @return true | false
     */
    public static boolean hasNoPrimary(TableMetadata tableMetadata) {
        return CollectionUtils.isEmpty(tableMetadata.getPrimaryMetas());
    }

    /**
     * check current primary column is digit key
     *
     * @param primaryKey primaryKey
     * @return true | false
     */
    public static boolean isDigitPrimaryKey(ColumnsMetaData primaryKey) {
        if (primaryKey.getColumnKey() != ColumnKey.PRI) {
            return false;
        }
        return numberDataTypes.contains(primaryKey.getDataType());
    }

    /**
     * Whether the current column type is a numeric type.
     *
     * @param columnKey column metadata
     * @return boolean
     */
    public static boolean isDigitKey(ColumnsMetaData columnKey) {
        return digitalDataTypes.contains(columnKey.getDataType()
                .toLowerCase(Locale.getDefault()));
    }

    public static boolean isDigitKey(String dataType) {
        return digitalDataTypes.contains(dataType.toLowerCase(Locale.getDefault()));
    }

    /**
     * Large numeric types; values may be represented in scientific notation.
     *
     * @param primaryKey primaryKey
     * @return boolean
     */
    public static boolean isLargeDigitalTypeKey(ColumnsMetaData primaryKey) {
        return LARGE_DIGITAL_TYPES.contains(primaryKey.getDataType()
                .toLowerCase(Locale.getDefault()));
    }

    public static boolean isInvalidPrimaryKey(ColumnsMetaData primaryKey) {
        if (primaryKey.getColumnKey() != ColumnKey.PRI) {
            return false;
        }
        return dataTypes.stream()
                .filter(dataType -> dataType.equalsIgnoreCase(primaryKey.getDataType()))
                .findAny()
                .isEmpty();
    }

    /**
     * Whether the given data type is a large-object type (CLOB/BLOB/text and the like).
     *
     * @param dataType column data type
     * @return true if the type is a large-object type
     */
    public static boolean isLargeColumnType(String dataType) {
        if (dataType == null) {
            return false;
        }
        return LARGE_COLUMN_TYPES.contains(dataType.toLowerCase(Locale.getDefault()));
    }

    /**
     * Return a copy of the metadata with large-object columns removed; primary key columns
     * are always kept so the filtered metadata stays usable for WHERE clauses.
     *
     * @param metadata original table metadata
     * @return filtered table metadata
     */
    public static TableMetadata filterLargeColumns(TableMetadata metadata) {
        if (metadata == null || CollectionUtils.isEmpty(metadata.getColumnsMetas())) {
            return metadata;
        }
        List<ColumnsMetaData> filteredColumns = metadata.getColumnsMetas().stream()
                .filter(col -> !isLargeColumnType(col.getDataType()))
                .collect(Collectors.toList());
        TableMetadata filtered = new TableMetadata();
        filtered.setTableName(metadata.getTableName());
        filtered.setSchema(metadata.getSchema());
        filtered.setColumnsMetas(filteredColumns);
        filtered.setPrimaryMetas(metadata.getPrimaryMetas());
        filtered.setOgCompatibilityB(metadata.isOgCompatibilityB());
        return filtered;
    }
}
