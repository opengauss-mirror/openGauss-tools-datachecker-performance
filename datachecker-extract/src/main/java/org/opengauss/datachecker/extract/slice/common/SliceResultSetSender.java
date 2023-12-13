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

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.task.ResultSetHandler;
import org.opengauss.datachecker.extract.task.ResultSetHandlerFactory;
import org.opengauss.datachecker.extract.util.HashHandler;
import org.opengauss.datachecker.extract.util.MetaDataUtil;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.NonNull;
import org.springframework.util.concurrent.ListenableFuture;

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
    protected static final Logger log = LogUtils.getBusinessLogger();
    private static final HashHandler hashHandler = new HashHandler();
    private final ResultSetHandler resultSetHandler;
    private final SliceKafkaAgents kafkaOperate;
    private final List<String> columns;
    private final List<String> primary;

    /**
     * constructor
     *
     * @param tableMetadata tableMetadata
     * @param kafkaOperate  kafkaOperate
     */
    public SliceResultSetSender(@NonNull TableMetadata tableMetadata, SliceKafkaAgents kafkaOperate) {
        this.resultSetHandler = new ResultSetHandlerFactory().createHandler(tableMetadata.getDataBaseType());
        this.columns = MetaDataUtil.getTableColumns(tableMetadata);
        this.primary = MetaDataUtil.getTablePrimaryColumns(tableMetadata);
        this.kafkaOperate = kafkaOperate;
    }

    /**
     * parse result set to RowDataHash
     *
     * @param rs  rs
     * @param sNo sNo
     */
    public void resultSetTranslateAndSend(String tableName, ResultSetMetaData rsmd, ResultSet rs,
        Map<String, String> result, int sNo) {
        RowDataHash dataHash = resultSetTranslate(tableName, rsmd, rs, result, sNo);
        kafkaOperate.sendRowData(dataHash);
    }

    public void resultSetTranslateAndSendRandom(String tableName, ResultSetMetaData rsmd, ResultSet rs,
        Map<String, String> result, int sNo) {
        RowDataHash dataHash = resultSetTranslate(tableName, rsmd, rs, result, sNo);
        kafkaOperate.sendRowDataRandomPartition(dataHash);
    }

    public ListenableFuture<SendResult<String, String>> resultSetTranslateAndSendSync(String tableName,
        ResultSetMetaData rsmd, ResultSet rs, Map<String, String> result, int sNo) {
        RowDataHash dataHash = resultSetTranslate(tableName, rsmd, rs, result, sNo);
        return kafkaOperate.sendRowDataSync(dataHash);
    }

    public RowDataHash resultSetTranslate(String tableName, ResultSetMetaData rsmd, ResultSet rs,
        Map<String, String> result, int sNo) {
        resultSetHandler.putOneResultSetToMap(tableName, rsmd, rs, result);
        RowDataHash dataHash = handler(primary, columns, result);
        dataHash.setSNo(sNo);
        result.clear();
        return dataHash;
    }

    public long checkOffsetEnd() {
        return kafkaOperate.checkTopicPartitionEndOffset();
    }

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
        long rowHash = hashHandler.xx3Hash(rowData, columns);
        String primaryValue = hashHandler.value(rowData, primary);
        long primaryHash = hashHandler.xx3Hash(rowData, primary);
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
        for (int idx = 0; idx < nextLine.length && idx < columns.size(); idx++) {
            result.put(columns.get(idx), nextLine[idx]);
        }
    }
}
