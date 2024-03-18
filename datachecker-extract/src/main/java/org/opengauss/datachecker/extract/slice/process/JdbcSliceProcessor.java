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
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.resource.ConnectionMgr;
import org.opengauss.datachecker.extract.resource.JdbcDataOperations;
import org.opengauss.datachecker.extract.slice.SliceProcessorContext;
import org.opengauss.datachecker.extract.slice.common.SliceResultSetSender;
import org.opengauss.datachecker.extract.task.sql.FullQueryStatement;
import org.opengauss.datachecker.extract.task.sql.QuerySqlEntry;
import org.opengauss.datachecker.extract.task.sql.SliceQueryStatement;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.StopWatch;
import org.springframework.util.concurrent.ListenableFuture;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
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
    private final AtomicInteger rowCount = new AtomicInteger(0);
    private final DruidDataSource dataSource;

    /**
     * JdbcSliceProcessor
     *
     * @param slice   slice
     * @param context context
     */
    public JdbcSliceProcessor(SliceVo slice, SliceProcessorContext context, DruidDataSource dataSource) {
        super(slice, context);
        this.jdbcOperation = context.getJdbcDataOperations();
        this.dataSource = dataSource;
    }

    @Override
    public void run() {
        LogUtils.info(log, "table slice [{}] is beginning to extract data", slice.toSimpleString());
        TableMetadata tableMetadata = context.getTableMetaData(table);
        SliceExtend sliceExtend = createSliceExtend(tableMetadata.getTableHash());
        try {
            QuerySqlEntry queryStatement = createQueryStatement(tableMetadata);
            LogUtils.debug(log, "table [{}] query statement :  {}", table, queryStatement.getSql());
            executeQueryStatement(queryStatement, tableMetadata, sliceExtend);
        } catch (Exception ex) {
            sliceExtend.setStatus(-1);
            LogUtils.error(log, "table slice [{}] is error", slice.toSimpleString(), ex);
        } finally {
            LogUtils.info(log, "table slice [{}] is finally ", slice.toSimpleString());
            feedbackStatus(sliceExtend);
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

    private void executeQueryStatement(QuerySqlEntry sqlEntry, TableMetadata tableMetadata, SliceExtend sliceExtend) {
        StopWatch stopWatch = new StopWatch(slice.getName());
        stopWatch.start("start " + slice.getName());
        SliceResultSetSender sliceSender = null;
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        try {
            long estimatedRowCount = slice.isSlice() ? slice.getFetchSize() : tableMetadata.getTableRows();
            long estimatedMemorySize = estimatedMemorySize(tableMetadata.getAvgRowLength(), estimatedRowCount);
            connection = jdbcOperation.tryConnectionAndClosedAutoCommit(estimatedMemorySize, dataSource);
            LogUtils.debug(log, "query slice and send data sql : {}", sqlEntry.getSql());
            ps = connection.prepareStatement(sqlEntry.getSql());
            resultSet = ps.executeQuery();
            resultSet.setFetchSize(FETCH_SIZE);
            sliceSender = createSliceResultSetSender(tableMetadata);
            sliceSender.setRecordSendKey(slice.getName());
            sliceExtend.setStartOffset(sliceSender.checkOffsetEnd());
            List<long[]> offsetList = sliceQueryResultAndSendSync(sliceSender, resultSet);
            updateExtendSliceOffsetAndCount(sliceExtend, rowCount.get(), offsetList);
            stopWatch.stop();
        } catch (Exception ex) {
            LogUtils.error(log, "slice [{}] has exception :", slice.getName(), ex);
            throw new ExtractDataAccessException(ex.getMessage());
        } finally {
            ConnectionMgr.close(connection, ps, resultSet);
            if (sliceSender != null) {
                sliceSender.agentsClosed();
            }
            jdbcOperation.releaseConnection(connection);
            LogUtils.info(log, "query slice and send data cost: {}", stopWatch.shortSummary());
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
            batchFutures.add(sliceSender.resultSetTranslateAndSendSync(rsmd, resultSet, result, slice.getNo()));
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
        return new SliceResultSetSender(tableMetadata, context.createSliceFixedKafkaAgents(topic, slice.getName()));
    }

    private void updateExtendSliceOffsetAndCount(SliceExtend sliceExtend, int rowCount, List<long[]> offsetList) {
        sliceExtend.setStartOffset(getMinOffset(offsetList));
        sliceExtend.setEndOffset(getMaxOffset(offsetList));
        sliceExtend.setCount(rowCount);
    }
}
