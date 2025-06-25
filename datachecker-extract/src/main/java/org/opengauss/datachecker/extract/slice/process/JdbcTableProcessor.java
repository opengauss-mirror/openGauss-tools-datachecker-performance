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

package org.opengauss.datachecker.extract.slice.process;

import com.alibaba.druid.pool.DruidDataSource;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.resource.JdbcDataOperations;
import org.opengauss.datachecker.extract.slice.SliceProcessorContext;
import org.opengauss.datachecker.extract.slice.common.SliceResultSetSender;
import org.opengauss.datachecker.extract.task.sql.AutoSliceQueryStatement;
import org.opengauss.datachecker.extract.task.sql.FullQueryStatement;
import org.opengauss.datachecker.extract.task.sql.QuerySqlEntry;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;

/**
 * JdbcTableProcessor
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/27
 * @since ：11
 */
public class JdbcTableProcessor extends AbstractTableProcessor {
    private static final Logger log = LogUtils.getLogger(JdbcTableProcessor.class);
    private final JdbcDataOperations jdbcOperation;
    private final DruidDataSource dataSource;
    private SliceResultSetSender sliceSender;

    /**
     * JdbcTableProcessor
     *
     * @param table table
     * @param context context
     */
    public JdbcTableProcessor(String table, SliceProcessorContext context, DruidDataSource dataSource) {
        super(table, context);
        this.jdbcOperation = context.getJdbcDataOperations();
        this.dataSource = dataSource;
    }

    @Override
    public void run() {
        SliceExtend tableSliceExtend = createTableSliceExtend();
        try {
            sliceSender = new SliceResultSetSender(tableMetadata, context.createSliceFixedKafkaAgents(topic, table));
            sliceSender.setRecordSendKey(table);
            long tableRowCount;
            if (noTableSlice()) {
                tableRowCount = executeFullTable(tableSliceExtend);
            } else {
                tableRowCount = executeMultiSliceTable(tableSliceExtend);
            }
            tableSliceExtend.setCount(tableRowCount);
            feedbackStatus(tableSliceExtend);
        } catch (Exception ex) {
            log.error("{}extract table processor error", ErrorCode.EXECUTE_SLICE_PROCESSOR, ex);
            tableSliceExtend.setStatus(-1);
            feedbackStatus(tableSliceExtend);
        } finally {
            Runtime.getRuntime().gc();
            sliceSender.agentsClosed();
        }
    }

    private long executeMultiSliceTable(SliceExtend tableSliceExtend) {
        final LocalDateTime start = LocalDateTime.now();
        Connection connection = null;
        List<QuerySqlEntry> querySqlList = getAutoSliceQuerySqlList();
        long tableRowCount = 0;
        int fetchSize = getFetchSize();
        try {
            long estimatedSize = estimatedMemorySize(tableMetadata.getAvgRowLength(), fetchSize);
            connection = jdbcOperation.tryConnectionAndClosedAutoCommit(estimatedSize);
            List<Long> minOffsetList = new LinkedList<>();
            List<Long> maxOffsetList = new LinkedList<>();
            for (int i = 0; i < querySqlList.size(); i++) {
                QuerySqlEntry sqlEntry = querySqlList.get(i);
                List<long[]> offsetList = new LinkedList<>();
                List<ListenableFuture<SendResult<String, String>>> batchFutures = new LinkedList<>();
                log.info(" {} , {}", table, sqlEntry.toString());
                try (PreparedStatement ps = connection.prepareStatement(sqlEntry.getSql());
                    ResultSet resultSet = ps.executeQuery()) {
                    resultSet.setFetchSize(fetchSize);
                    ResultSetMetaData rsmd = resultSet.getMetaData();
                    int rowCount = 0;
                    while (resultSet.next()) {
                        rowCount++;
                        batchFutures.add(sliceSender.resultSetTranslateAndSendSync(rsmd, resultSet, i));
                        if (batchFutures.size() == FETCH_SIZE) {
                            offsetList.add(getBatchFutureRecordOffsetScope(batchFutures));
                            batchFutures.clear();
                        }
                    }
                    if (!batchFutures.isEmpty()) {
                        offsetList.add(getBatchFutureRecordOffsetScope(batchFutures));
                        batchFutures.clear();
                    }
                    minOffsetList.add(getMinOffset(offsetList));
                    maxOffsetList.add(getMaxOffset(offsetList));
                    sliceSender.resultFlush();
                    tableRowCount += rowCount;
                    log.info("finish {} - {} - {}, {}", table, i, rowCount, tableRowCount);
                }
            }
            tableSliceExtend.setStartOffset(getMinMinOffset(minOffsetList));
            tableSliceExtend.setEndOffset(getMaxMaxOffset(maxOffsetList));
            tableSliceExtend.setCount(tableRowCount);
        } catch (SQLException ex) {
            log.error("{}jdbc query  {} error : {}", ErrorCode.EXECUTE_QUERY_SQL, table, ex.getMessage());
            throw new ExtractDataAccessException();
        } finally {
            jdbcOperation.releaseConnection(connection);
            log.info("query table [{}] row-count [{}] cost [{}] milliseconds", table, tableRowCount,
                Duration.between(start, LocalDateTime.now()).toMillis());
        }
        return tableRowCount;
    }

    private long executeFullTable(SliceExtend tableSliceExtend) {
        final LocalDateTime start = LocalDateTime.now();
        Connection connection = null;
        long tableRowCount = 0;
        int fetchSize = getFetchSize();
        try {
            long estimatedSize = estimatedMemorySize(tableMetadata.getAvgRowLength(), fetchSize);
            connection = jdbcOperation.tryConnectionAndClosedAutoCommit(estimatedSize);
            QuerySqlEntry sqlEntry = getFullQuerySqlEntry();
            log.info(" {} , {}", table, sqlEntry.toString());
            List<long[]> offsetList = new LinkedList<>();
            List<ListenableFuture<SendResult<String, String>>> batchFutures = new LinkedList<>();
            try (PreparedStatement ps = connection.prepareStatement(sqlEntry.getSql());
                ResultSet resultSet = ps.executeQuery()) {
                resultSet.setFetchSize(fetchSize);
                ResultSetMetaData rsmd = resultSet.getMetaData();
                while (resultSet.next()) {
                    tableRowCount++;
                    batchFutures.add(sliceSender.resultSetTranslateAndSendSync(rsmd, resultSet, 0));
                    if (batchFutures.size() == FETCH_SIZE) {
                        offsetList.add(getBatchFutureRecordOffsetScope(batchFutures));
                        batchFutures.clear();
                    }
                }
                if (!batchFutures.isEmpty()) {
                    offsetList.add(getBatchFutureRecordOffsetScope(batchFutures));
                    batchFutures.clear();
                }
                tableSliceExtend.setStartOffset(getMinOffset(offsetList));
                tableSliceExtend.setEndOffset(getMaxOffset(offsetList));
                tableSliceExtend.setCount(tableRowCount);
                log.info("finish {} , {}", table, tableRowCount);
            }
        } catch (SQLException ex) {
            log.error("{}jdbc query  {} error : {}", ErrorCode.EXECUTE_QUERY_SQL, table, ex.getMessage());
            throw new ExtractDataAccessException();
        } finally {
            jdbcOperation.releaseConnection(connection);
            log.info("query table [{}] row-count [{}] cost [{}] milliseconds", table, tableRowCount,
                Duration.between(start, LocalDateTime.now()).toMillis());
        }
        return tableRowCount;
    }

    private QuerySqlEntry getFullQuerySqlEntry() {
        FullQueryStatement queryStatement = context.createFullQueryStatement();
        return queryStatement.builderByTaskOffset(tableMetadata);
    }

    private List<QuerySqlEntry> getAutoSliceQuerySqlList() {
        // 单主键根据主键进行SQL分片，联合主键根据第一主键值进行SQL分片
        AutoSliceQueryStatement statement = context.createAutoSliceQueryStatement(tableMetadata.getTableName());
        return statement.builderByTaskOffset(tableMetadata, getMaximumTableSliceSize());
    }
}
