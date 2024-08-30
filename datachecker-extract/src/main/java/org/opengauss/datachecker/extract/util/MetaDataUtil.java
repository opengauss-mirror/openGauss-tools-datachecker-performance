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
     * 判断当前列类型是否是数字类型
     *
     * @param columnKey 列元数据
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
     * 大数字类型，结果可能为科学计数表示
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
}
