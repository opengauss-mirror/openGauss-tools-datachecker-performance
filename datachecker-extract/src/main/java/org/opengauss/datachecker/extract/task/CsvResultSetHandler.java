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

package org.opengauss.datachecker.extract.task;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.util.MetaDataUtil;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * CSV Result set object processor
 *
 * @author wang chao
 * @date ：Created in 2022/6/13
 * @since 11
 **/
public class CsvResultSetHandler {
    private static final Logger LOG = LogUtils.getLogger();
    private static final String CSV_NULL_VALUE = "null";
    private static final String BINARY_TYPE = "binary";
    private static final String BLOB_TYPE = "blob";

    private final ResultParseConsumer<String, Map<String, String>, ColumnsMetaData> largeDigitalType =
        (value, result, column) -> {
            if (isScientificNotation(value)) {
                result.put(column.getColumnName(), new BigDecimal(value).toPlainString());
            } else {
                result.put(column.getColumnName(), value);
            }
        };
    private final ResultParseConsumer<String, Map<String, String>, ColumnsMetaData> defaultConsumer =
        (value, result, column) -> result.put(column.getColumnName(), value);
    private final ResultParseConsumer<String, Map<String, String>, ColumnsMetaData> binaryAndBlobConsumer =
        (value, result, column) -> result.put(column.getColumnName(), value.substring(1));
    private final ResultParseConsumer<String, Map<String, String>, ColumnsMetaData> csvNullValueConsumer =
        (value, result, column) -> result.put(column.getColumnName(), CSV_NULL_VALUE);

    /**
     * parse csv line to column map
     *
     * @param columnMetas columnMetas
     * @param nextLine    nextLine
     * @param values      values
     */
    public void putOneResultSetToMap(List<ColumnsMetaData> columnMetas, String[] nextLine, Map<String, String> values) {
        int idx;
        String tmpValue;
        for (ColumnsMetaData column : columnMetas) {
            idx = column.getOrdinalPosition() - 1;
            tmpValue = nextLine[idx];
            if (CSV_NULL_VALUE.equalsIgnoreCase(tmpValue)) {
                csvNullValueConsumer.accept(tmpValue, values, column);
            } else {
                if (MetaDataUtil.isLargeDigitalTypeKey(column)) {
                    largeDigitalType.accept(tmpValue, values, column);
                } else if (isBinaryOrBlob(column.getColumnType())) {
                    binaryAndBlobConsumer.accept(tmpValue, values, column);
                } else {
                    defaultConsumer.accept(tmpValue, values, column);
                }
            }
        }
    }

    private boolean isScientificNotation(String value) {
        return value.contains("E") || value.contains("e");
    }

    /**
     * 判断当前类型是否是binary（binary/varbinary） 或者 blob(blob,tinyblob,blob,mediumblob,longblob) 类型
     *
     * @param columnType CSV场景加载表元数据类型
     * @return boolean
     */
    private boolean isBinaryOrBlob(String columnType) {
        return columnType.contains(BINARY_TYPE) || columnType.contains(BLOB_TYPE);
    }

    /**
     * ResultParseConsumer
     *
     * @param <S> result row original text
     * @param <M> parse result map
     * @param <C> column metadata
     */
    @FunctionalInterface
    private interface ResultParseConsumer<S, M, C> {
        /**
         * ResultParseConsumer
         *
         * @param value  result row original text
         * @param result parse result map
         * @param column column metadata
         */
        void accept(String value, Map<String, String> result, ColumnsMetaData column);
    }
}