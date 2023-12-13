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

import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;
import org.opengauss.datachecker.extract.resource.JdbcDataOperations;
import org.opengauss.datachecker.extract.slice.SliceProcessorContext;
import org.opengauss.datachecker.extract.slice.common.SliceResultSetSender;
import org.opengauss.datachecker.extract.task.sql.AutoSliceQueryStatement;
import org.opengauss.datachecker.extract.task.sql.FullQueryStatement;
import org.opengauss.datachecker.extract.task.sql.QuerySqlEntry;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * JdbcTableProcessor
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/27
 * @since ：11
 */
public class JdbcTableProcessor extends AbstractTableProcessor {
    private final JdbcDataOperations jdbcOperation;

    private SliceResultSetSender sliceSender;

    public JdbcTableProcessor(DataSource datasource, String table, SliceProcessorContext context) {
        super(table, context);
        this.jdbcOperation = context.getJdbcDataOperations(datasource);
    }

    @Override
    public void run() {
        SliceExtend tableSliceExtend = createTableSliceExtend();
        try {
            initTableMetadata();
            sliceSender = new SliceResultSetSender(tableMetadata, context.createSliceKafkaAgents(table));
            long tableRowCount;
            if (noTableSlice()) {
                tableRowCount = executeFullTable();
            } else {
                tableRowCount = executeMultiSliceTable();
            }
            tableSliceExtend.setCount(tableRowCount);
            feedbackStatus(tableSliceExtend);
        } catch (Exception ex) {
            log.error("extract", ex);
            tableSliceExtend.setStatus(-1);
            feedbackStatus(tableSliceExtend);
        } finally {
            Runtime.getRuntime().gc();
            sliceSender.agentsClosed();
        }
    }

    private long executeMultiSliceTable() {
        final LocalDateTime start = LocalDateTime.now();
        Connection connection = null;
        List<QuerySqlEntry> querySqlList = getAutoSliceQuerySqlList();
        long tableRowCount = 0;
        int fetchSize = getFetchSize();
        try {
            long estimatedSize = estimatedMemorySize(tableMetadata.getAvgRowLength(), fetchSize);
            connection = jdbcOperation.tryConnectionAndClosedAutoCommit(estimatedSize);
            for (int i = 0; i < querySqlList.size(); i++) {
                QuerySqlEntry sqlEntry = querySqlList.get(i);
                log.info(" {} , {}", table, sqlEntry.toString());
                try (PreparedStatement ps = connection.prepareStatement(sqlEntry.getSql());
                    ResultSet resultSet = ps.executeQuery()) {
                    resultSet.setFetchSize(fetchSize);
                    ResultSetMetaData rsmd = resultSet.getMetaData();
                    Map<String, String> result = new TreeMap<>();
                    int rowCount = 0;
                    while (resultSet.next()) {
                        rowCount++;
                        sliceSender.resultSetTranslateAndSendRandom(table, rsmd, resultSet, result, i);
                    }
                    sliceSender.resultFlush();
                    tableRowCount += rowCount;
                    log.info("finish {} - {} - {}, {}", table, i, rowCount, tableRowCount);
                }
            }
        } catch (SQLException ex) {
            log.error("jdbc query  {} error : {}", table, ex.getMessage());
            throw new ExtractDataAccessException();
        } finally {
            jdbcOperation.releaseConnection(connection);
            log.info("query table [{}] row-count [{}] cost [{}] milliseconds", table, tableRowCount,
                Duration.between(start, LocalDateTime.now()).toMillis());
        }
        return tableRowCount;
    }

    private long executeFullTable() {
        final LocalDateTime start = LocalDateTime.now();
        Connection connection = null;
        long tableRowCount = 0;
        int fetchSize = getFetchSize();
        try {
            long estimatedSize = estimatedMemorySize(tableMetadata.getAvgRowLength(), fetchSize);
            connection = jdbcOperation.tryConnectionAndClosedAutoCommit(estimatedSize);
            QuerySqlEntry sqlEntry = getFullQuerySqlEntry();
            log.info(" {} , {}", table, sqlEntry.toString());
            try (PreparedStatement ps = connection.prepareStatement(sqlEntry.getSql());
                ResultSet resultSet = ps.executeQuery()) {
                resultSet.setFetchSize(fetchSize);
                ResultSetMetaData rsmd = resultSet.getMetaData();
                Map<String, String> result = new TreeMap<>();
                int rowCount = 0;
                while (resultSet.next()) {
                    rowCount++;
                    sliceSender.resultSetTranslateAndSendRandom(table, rsmd, resultSet, result, 0);
                }
                tableRowCount += rowCount;
                log.info("finish {} , {}: {}", table, rowCount, tableRowCount);
            }
        } catch (SQLException ex) {
            log.error("jdbc query  {} error : {}", table, ex.getMessage());
            throw new ExtractDataAccessException();
        } finally {
            jdbcOperation.releaseConnection(connection);
            log.info("query table [{}] row-count [{}] cost [{}] milliseconds", table, tableRowCount,
                Duration.between(start, LocalDateTime.now())
                        .toMillis());
        }
        return tableRowCount;
    }

    private QuerySqlEntry getFullQuerySqlEntry() {
        FullQueryStatement queryStatement = context.createFullQueryStatement();
        return queryStatement.builderByTaskOffset(tableMetadata);
    }

    private List<QuerySqlEntry> getAutoSliceQuerySqlList() {
        AutoSliceQueryStatement statement = context.createAutoSliceQueryStatement(tableMetadata);
        return statement.builderByTaskOffset(tableMetadata, getMaximumTableSliceSize());
    }
}
