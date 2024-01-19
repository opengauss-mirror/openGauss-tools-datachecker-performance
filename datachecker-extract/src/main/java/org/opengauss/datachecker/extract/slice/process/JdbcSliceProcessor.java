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
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;
import org.opengauss.datachecker.extract.resource.JdbcDataOperations;
import org.opengauss.datachecker.extract.slice.SliceProcessorContext;
import org.opengauss.datachecker.extract.slice.common.SliceResultSetSender;
import org.opengauss.datachecker.extract.task.sql.FullQueryStatement;
import org.opengauss.datachecker.extract.task.sql.QuerySqlEntry;
import org.opengauss.datachecker.extract.task.sql.SliceQueryStatement;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JdbcSliceProcessor
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/8
 * @since ：11
 */
public class JdbcSliceProcessor extends AbstractSliceProcessor {
    private final JdbcDataOperations jdbcOperation;
    private final String table;
    private final AtomicInteger rowCount = new AtomicInteger(0);

    /**
     * JdbcSliceProcessor
     *
     * @param slice   slice
     * @param context context
     */
    public JdbcSliceProcessor(SliceVo slice, SliceProcessorContext context) {
        super(slice, context);
        this.jdbcOperation = context.getJdbcDataOperations();
        this.table = slice.getTable();
        this.rowCount.set(0);
    }

    @Override
    public void run() {
        log.info("table slice [{}] is beginning to extract data", slice.toSimpleString());
        try {
            TableMetadata tableMetadata = context.getTableMetaData(table);
            SliceExtend sliceExtend = createSliceExtend(tableMetadata.getTableHash());
            if (!slice.isEmptyTable()) {
                QuerySqlEntry queryStatement = createQueryStatement(tableMetadata);
                log.debug("table [{}] query statement :  {}", table, queryStatement.getSql());
                executeQueryStatement(queryStatement, tableMetadata, sliceExtend);
            } else {
                log.info("table slice [{}] is empty ", slice.toSimpleString());
            }
            feedbackStatus(sliceExtend);
        } catch (Exception ex) {
            log.error("table slice [{}] is error", slice.toSimpleString(), ex);
        } finally {
            log.info("table slice [{}] is finally ", slice.toSimpleString());
            context.saveProcessing(slice);
        }
    }

    private QuerySqlEntry createQueryStatement(TableMetadata tableMetadata) {
        if (slice.isSlice()) {
            SliceQueryStatement sliceStatement = context.createSliceQueryStatement();
            return sliceStatement.buildSlice(tableMetadata, slice);
        } else {
            FullQueryStatement queryStatement = context.createFullQueryStatement();
            return queryStatement.builderByTaskOffset(tableMetadata);
        }
    }

    private SliceExtend executeQueryStatement(QuerySqlEntry sqlEntry, TableMetadata tableMetadata,
        SliceExtend sliceExtend) {
        final LocalDateTime start = LocalDateTime.now();
        Connection connection = null;
        long jdbcQueryCost = 0;
        long sendDataCost = 0;
        long sliceAllCost = 0;

        try {
            long estimatedRowCount = slice.isSlice() ? slice.getFetchSize() : tableMetadata.getTableRows();
            long estimatedMemorySize = estimatedMemorySize(tableMetadata.getAvgRowLength(), estimatedRowCount);
            connection = jdbcOperation.tryConnectionAndClosedAutoCommit(estimatedMemorySize);
            log.debug("slice [{}] fetch jdbc connection.", slice.getName());
            SliceResultSetSender sliceSender = createSliceResultSetSender(tableMetadata);
            sliceExtend.setStartOffset(sliceSender.checkOffsetEnd());

            try (PreparedStatement ps = connection.prepareStatement(sqlEntry.getSql());
                ResultSet resultSet = ps.executeQuery()) {
                log.debug("slice [{}] jdbc execute query complete.", slice.getName());
                LocalDateTime jdbcQuery = LocalDateTime.now();
                jdbcQueryCost = durationBetweenToMillis(start, jdbcQuery);
                resultSet.setFetchSize(FETCH_SIZE);
                List<long[]> offsetList = sliceQueryResultAndSendSync(sliceSender, resultSet);
                updateExtendSliceOffsetAndCount(sliceExtend, rowCount.get(), offsetList);
                sendDataCost = durationBetweenToMillis(jdbcQuery, LocalDateTime.now());
            }
            return sliceExtend;
        } catch (SQLException ex) {
            throw new ExtractDataAccessException(ex);
        } finally {
            jdbcOperation.releaseConnection(connection);
            log.debug("slice [{}] release jdbc connection.", slice.getName());
            sliceAllCost = durationBetweenToMillis(start, LocalDateTime.now());
            log.info("query slice [{}] cost [{} /{} /{}] milliseconds", sliceExtend.toSimpleString(), jdbcQueryCost,
                sendDataCost, sliceAllCost);
        }
    }

    private List<long[]> sliceQueryResultAndSendSync(SliceResultSetSender sliceSender, ResultSet resultSet)
        throws SQLException {
        ResultSetMetaData rsmd = resultSet.getMetaData();
        Map<String, String> result = new TreeMap<>();
        List<long[]> offsetList = new LinkedList<>();
        List<ListenableFuture<SendResult<String, String>>> batchFutures = new LinkedList<>();
        while (resultSet.next()) {
            this.rowCount.incrementAndGet();
            batchFutures.add(sliceSender.resultSetTranslateAndSendSync(table, rsmd, resultSet, result, slice.getNo()));
            if (batchFutures.size() == FETCH_SIZE) {
                offsetList.add(getBatchFutureRecordOffsetScope(batchFutures));
                batchFutures.clear();
            }
        }
        if (batchFutures.size() > 0) {
            offsetList.add(getBatchFutureRecordOffsetScope(batchFutures));
            batchFutures.clear();
        }
        return offsetList;
    }

    private SliceResultSetSender createSliceResultSetSender(TableMetadata tableMetadata) {
        return new SliceResultSetSender(tableMetadata, context.createSliceKafkaAgents(slice));
    }

    private void updateExtendSliceOffsetAndCount(SliceExtend sliceExtend, int rowCount, List<long[]> offsetList) {
        sliceExtend.setStartOffset(getMinOffset(offsetList));
        sliceExtend.setEndOffset(getMaxOffset(offsetList));
        sliceExtend.setCount(rowCount);
    }
}
