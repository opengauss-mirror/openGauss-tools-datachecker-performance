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

package org.opengauss.datachecker.extract.slice.common;

import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.task.ResultSetHandler;
import org.opengauss.datachecker.extract.task.ResultSetHandlerFactory;
import org.opengauss.datachecker.extract.util.HashHandler;
import org.opengauss.datachecker.extract.util.MetaDataUtil;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.NonNull;
import org.springframework.util.concurrent.ListenableFuture;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.Map;

/**
 * SliceResultSetSender
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/9
 * @since ：11
 */
public class SliceResultSetSender {
    private static final HashHandler HASH_HANDLER = new HashHandler();
    private static final String CSV_NULL_VALUE = "null";
    private static final String BINARY_TYPE = "binary";
    private static final String BLOB_TYPE = "blob";
    private final ResultSetHandler resultSetHandler;
    private final SliceKafkaAgents kafkaOperate;
    private final List<String> columns;
    private final List<ColumnsMetaData> columnMetas;
    private final List<String> primary;
    private final String tableName;
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
     * constructor
     *
     * @param tableMetadata tableMetadata
     * @param kafkaOperate  kafkaOperate
     */
    public SliceResultSetSender(@NonNull TableMetadata tableMetadata, SliceKafkaAgents kafkaOperate) {
        this.resultSetHandler = new ResultSetHandlerFactory().createHandler(tableMetadata.getDataBaseType());
        this.columns = MetaDataUtil.getTableColumns(tableMetadata);
        this.columnMetas = tableMetadata.getColumnsMetas();
        this.primary = MetaDataUtil.getTablePrimaryColumns(tableMetadata);
        this.tableName = tableMetadata.getTableName();
        this.kafkaOperate = kafkaOperate;
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

    /**
     * resultSetTranslateAndSendRandom
     *
     * @param rsmd   rsmd
     * @param rs     rs
     * @param result result
     * @param sNo    sNo
     */
    public void resultSetTranslateAndSendRandom(ResultSetMetaData rsmd, ResultSet rs, Map<String, String> result,
        int sNo) {
        RowDataHash dataHash = resultSetTranslate(rsmd, rs, result, sNo);
        kafkaOperate.sendRowDataRandomPartition(dataHash);
    }

    /**
     * resultSetTranslateAndSendSync
     *
     * @param rsmd   rsmd
     * @param rs     rs
     * @param result result
     * @param sNo    sNo
     */
    public ListenableFuture<SendResult<String, String>> resultSetTranslateAndSendSync(ResultSetMetaData rsmd,
        ResultSet rs, Map<String, String> result, int sNo) {
        RowDataHash dataHash = resultSetTranslate(rsmd, rs, result, sNo);
        return kafkaOperate.sendRowDataSync(dataHash);
    }

    /**
     * resultSetTranslate
     *
     * @param rsmd   rsmd
     * @param rs     rs
     * @param result result
     * @param sNo    sNo
     */
    public RowDataHash resultSetTranslate(ResultSetMetaData rsmd, ResultSet rs, Map<String, String> result, int sNo) {
        resultSetHandler.putOneResultSetToMap(tableName, rsmd, rs, result);
        RowDataHash dataHash = handler(primary, columns, result);
        dataHash.setSNo(sNo);
        result.clear();
        return dataHash;
    }

    /**
     * checkOffsetEnd
     *
     * @return checkOffsetEnd
     */
    public long checkOffsetEnd() {
        return kafkaOperate.checkTopicPartitionEndOffset();
    }

    /**
     * agentsClosed
     */
    public void agentsClosed() {
        kafkaOperate.agentsClosed();
    }

    /**
     * <pre>
     * Obtain the primary key information in the ResultSet according to the primary key name of the table.
     * Obtain all field information in the ResultSet according to the set of table field names.
     * And hash the primary key value and the record value.
     * The calculation result is encapsulated as a RowDataHash object
     * </pre>
     *
     * @param primary primary list
     * @param columns column list
     * @param rowData Query data set
     * @return Returns the hash calculation result of extracted data
     */
    private RowDataHash handler(List<String> primary, List<String> columns, Map<String, String> rowData) {
        long rowHash = HASH_HANDLER.xx3Hash(rowData, columns);
        String primaryValue = HASH_HANDLER.value(rowData, primary);
        long primaryHash = HASH_HANDLER.xx3Hash(rowData, primary);
        RowDataHash hashData = new RowDataHash();
        hashData.setKey(primaryValue)
                .setKHash(primaryHash)
                .setVHash(rowHash);
        return hashData;
    }

    /**
     * kafka send flush
     */
    public synchronized void resultFlush() {
        kafkaOperate.flush();
    }

    /**
     * csv mode, translate next line data to map and send it to kafka topic
     *
     * @param nextLine next line
     * @param result   temp map
     * @param rowIdx   row idx of csv file
     * @param sNo      sNo
     */
    public void csvTranslateAndSend(String[] nextLine, Map<String, String> result, int rowIdx, int sNo) {
        RowDataHash dataHash = csvTranslate(nextLine, result, rowIdx, sNo);
        kafkaOperate.sendRowData(dataHash);
    }

    public void csvTranslateAndSendRandom(String[] nextLine, Map<String, String> result, int rowIdx, int sNo) {
        RowDataHash dataHash = csvTranslate(nextLine, result, rowIdx, sNo);
        kafkaOperate.sendRowDataRandomPartition(dataHash);
    }

    /**
     * csv mode, translate next line data to map and send it to kafka topic
     *
     * @param nextLine next line
     * @param result   temp map
     * @param rowIdx   row idx of csv file
     * @param sNo      sNo
     */
    public ListenableFuture<SendResult<String, String>> csvTranslateAndSendSync(String[] nextLine,
        Map<String, String> result, int rowIdx, int sNo) {
        RowDataHash dataHash = csvTranslate(nextLine, result, rowIdx, sNo);
        return kafkaOperate.sendRowDataSync(dataHash);
    }

    private RowDataHash csvTranslate(String[] nextLine, Map<String, String> result, int rowIdx, int sNo) {
        parse(nextLine, result);
        RowDataHash dataHash = handler(primary, columns, result);
        dataHash.setIdx(rowIdx);
        dataHash.setSNo(sNo);
        result.clear();
        return dataHash;
    }

    private void parse(String[] nextLine, Map<String, String> result) {
        int idx;
        String tmpValue;
        for (ColumnsMetaData column : columnMetas) {
            idx = column.getOrdinalPosition() - 1;
            tmpValue = nextLine[idx];
            if (CSV_NULL_VALUE.equalsIgnoreCase(tmpValue)) {
                csvNullValueConsumer.accept(tmpValue, result, column);
            } else {
                if (isBinaryOrBlob(column.getColumnType())) {
                    binaryAndBlobConsumer.accept(tmpValue, result, column);
                } else if (MetaDataUtil.isLargeDigitalTypeKey(column)) {
                    largeDigitalType.accept(tmpValue, result, column);
                } else {
                    defaultConsumer.accept(tmpValue, result, column);
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
}
