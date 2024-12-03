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
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
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
import org.opengauss.datachecker.extract.task.sql.UnionPrimarySliceQueryStatement;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * JdbcSliceProcessor
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/8
 * @since ：11
 */
public class JdbcSliceProcessor extends AbstractSliceProcessor {
    private static final Logger log = LogUtils.getLogger(JdbcSliceProcessor.class);
    private final JdbcDataOperations jdbcOperation;
    private final AtomicInteger rowCount = new AtomicInteger(0);
    private final DruidDataSource dataSource;

    /**
     * JdbcSliceProcessor
     *
     * @param slice slice
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
            if (tableMetadata.isUnionPrimary()) {
                DataBaseType dataBaseType = ConfigCache.getValue(ConfigConstants.DATA_BASE_TYPE, DataBaseType.class);
                Assert.isTrue(isSuiteUnionPrimary(dataBaseType),
                    "Union primary is not supported by current database type " + dataBaseType.getDescription());
                executeSliceQueryStatementPage(tableMetadata, sliceExtend);
            } else {
                QuerySqlEntry queryStatement = createQueryStatement(tableMetadata);
                LogUtils.debug(log, "table [{}] query statement :  {}", table, queryStatement.getSql());
                executeQueryStatement(queryStatement, tableMetadata, sliceExtend);
            }
        } catch (Exception | Error ex) {
            sliceExtend.setStatus(-1);
            LogUtils.error(log, "table slice [{}] is error", slice.toSimpleString(), ex);
        } finally {
            LogUtils.info(log, "table slice [{}] is finally  ", slice.toSimpleString());
            feedbackStatus(sliceExtend);
            context.saveProcessing(slice);
        }
    }

    private boolean isSuiteUnionPrimary(DataBaseType dataBaseType) {
        return Objects.equals(dataBaseType, DataBaseType.OG) || Objects.equals(dataBaseType, DataBaseType.MS);
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

    private void executeSliceQueryStatementPage(TableMetadata tableMetadata, SliceExtend sliceExtend) {
        // 分片数据统计
        UnionPrimarySliceQueryStatement sliceStatement = context.createSlicePageQueryStatement();
        QuerySqlEntry sliceCountSql = sliceStatement.buildSliceCount(tableMetadata, slice);
        int sliceCount = querySliceRowTotalCount(sliceExtend, sliceCountSql);
        QuerySqlEntry baseSliceSql = sliceStatement.buildSlice(tableMetadata, slice);
        List<String> pageStatementList = sliceStatement.buildPageStatement(baseSliceSql, sliceCount,
            slice.getFetchSize());
        SliceResultSetSender sliceSender = null;
        Connection connection = null;
        try {
            // 申请数据库链接
            long estimatedRowCount = slice.isSlice() ? slice.getFetchSize() : tableMetadata.getTableRows();
            long estimatedMemorySize = estimatedMemorySize(tableMetadata.getAvgRowLength(), estimatedRowCount);
            connection = jdbcOperation.tryConnectionAndClosedAutoCommit(estimatedMemorySize, dataSource);
            // 获取连接，准备查询分片数据： 并开启数据异步处理线程
            sliceSender = createSliceResultSetSender(tableMetadata);
            sliceSender.setRecordSendKey(slice.getName());
            List<long[]> offsetList = new CopyOnWriteArrayList<>();
            List<ListenableFuture<SendResult<String, String>>> batchFutures = new CopyOnWriteArrayList<>();
            AsyncDataHandler asyncHandler = new AsyncDataHandler(batchFutures, sliceSender, offsetList);
            asyncHandler.start();
            context.asyncSendSlice(asyncHandler);
            // 开始查询数据，并将结果推送到异步处理线程中。
            boolean isFirstStatement = true;
            long startOffset = 0L;
            for (String pageStatement : pageStatementList) {
                if (isFirstStatement) {
                    // only use first page statement's start offset
                    startOffset = pageQueryUnionPrimarySlice(pageStatement, connection, sliceSender, asyncHandler);
                } else {
                    // other page statement's start offset is ignored
                    pageQueryUnionPrimarySlice(pageStatement, connection, sliceSender, asyncHandler);
                }
                isFirstStatement = false;
            }
            sliceExtend.setStartOffset(startOffset);
            waitToStopAsyncHandlerAndResources(asyncHandler);
            updateExtendSliceOffsetAndCount(sliceExtend, rowCount.get(), offsetList);
        } catch (Exception ex) {
            LogUtils.error(log, "slice [{}] has exception :", slice.getName(), ex);
            throw new ExtractDataAccessException(ex.getMessage());
        } finally {
            ConnectionMgr.close(connection, null, null);
            if (sliceSender != null) {
                sliceSender.agentsClosed();
            }
            jdbcOperation.releaseConnection(connection);
            LogUtils.info(log, "query union primary slice and send data {} Count:{}", sliceExtend.getName(),
                rowCount.get());
        }
    }

    private long pageQueryUnionPrimarySlice(String pageStatement, Connection connection,
        SliceResultSetSender sliceSender, AsyncDataHandler asyncHandler) throws SQLException, InterruptedException {
        long startOffset;
        PreparedStatement ps = connection.prepareStatement(pageStatement);
        ps.setFetchSize(FETCH_SIZE);
        ResultSet resultSet = ps.executeQuery();
        startOffset = sliceSender.checkOffsetEnd();
        ResultSetMetaData rsmd = resultSet.getMetaData();
        while (resultSet.next()) {
            this.rowCount.incrementAndGet();
            if (asyncHandler.isSenderBusy()) {
                Thread.sleep(100);
            }
            asyncHandler.addRow(sliceSender.resultSet(rsmd, resultSet));
        }
        // 数据发送到异步处理线程中，关闭ps与rs
        ConnectionMgr.close(null, ps, resultSet);
        return startOffset;
    }

    private static void waitToStopAsyncHandlerAndResources(AsyncDataHandler asyncHandler) {
        // 全部分页查询处理完成，关闭数据库连接，并关闭异步数据处理线程
        try {
            asyncHandler.waitToStop();
        } catch (InterruptedException e) {
            throw new ExtractDataAccessException("slice data async handler is interrupted");
        }
    }

    private int querySliceRowTotalCount(SliceExtend sliceExtend, QuerySqlEntry sliceCountSql) {
        int sliceCount = 0;
        try (Connection connection = jdbcOperation.tryConnectionAndClosedAutoCommit(1L, dataSource);
            PreparedStatement ps = connection.prepareStatement(sliceCountSql.getSql());
            ResultSet resultSet = ps.executeQuery();) {
            if (resultSet.next()) {
                sliceCount = resultSet.getInt(1);
            }
        } catch (SQLException ex) {
            log.error("execute slice count query error ", ex);
            throw new ExtractDataAccessException("execute slice count query error");
        }
        log.info("query union primary table slice {} Count:{}", sliceExtend.getName(), sliceCount);
        return sliceCount;
    }

    private void executeQueryStatement(QuerySqlEntry sqlEntry, TableMetadata tableMetadata, SliceExtend sliceExtend) {
        SliceResultSetSender sliceSender = null;
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        try {
            // 申请数据库链接
            long estimatedRowCount = slice.isSlice() ? slice.getFetchSize() : tableMetadata.getTableRows();
            long estimatedMemorySize = estimatedMemorySize(tableMetadata.getAvgRowLength(), estimatedRowCount);
            connection = jdbcOperation.tryConnectionAndClosedAutoCommit(estimatedMemorySize, dataSource);
            // 获取连接，准备查询分片数据： 并开启数据异步处理线程
            List<long[]> offsetList = new CopyOnWriteArrayList<>();
            List<ListenableFuture<SendResult<String, String>>> batchFutures = new CopyOnWriteArrayList<>();
            sliceSender = createSliceResultSetSender(tableMetadata);
            sliceSender.setRecordSendKey(slice.getName());
            AsyncDataHandler asyncHandler = new AsyncDataHandler(batchFutures, sliceSender, offsetList);
            asyncHandler.start();
            context.asyncSendSlice(asyncHandler);
            // 开始查询数据，并将结果推送到异步处理线程中。
            ps = connection.prepareStatement(sqlEntry.getSql());
            ps.setFetchSize(FETCH_SIZE);
            resultSet = ps.executeQuery();
            sliceExtend.setStartOffset(sliceSender.checkOffsetEnd());
            ResultSetMetaData rsmd = resultSet.getMetaData();
            while (resultSet.next()) {
                this.rowCount.incrementAndGet();
                if (asyncHandler.isSenderBusy()) {
                    Thread.sleep(100);
                }
                // 数据发送到异步处理线程
                asyncHandler.addRow(sliceSender.resultSet(rsmd, resultSet));
            }
            // 全部分片查询处理完成，关闭数据库连接，并关闭异步数据处理线程 ，关闭ps与rs
            try {
                ConnectionMgr.close(null, ps, resultSet);
                asyncHandler.waitToStop();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            updateExtendSliceOffsetAndCount(sliceExtend, rowCount.get(), offsetList);
        } catch (Exception ex) {
            LogUtils.error(log, "slice [{}] has exception :", slice.getName(), ex);
            throw new ExtractDataAccessException(ex.getMessage());
        } finally {
            ConnectionMgr.close(connection, ps, resultSet);
            if (sliceSender != null) {
                sliceSender.agentsClosed();
            }
            jdbcOperation.releaseConnection(connection);
            LogUtils.info(log, "query slice and send data count {}", rowCount.get());
        }
    }

    /**
     * async data handler thread
     */
    class AsyncDataHandler implements Runnable {
        private final List<ListenableFuture<SendResult<String, String>>> batchFutures;
        private final SliceResultSetSender sliceSender;
        private final int maxQueueSize = 10000;
        private final BlockingQueue<Map<String, String>> batchData = new LinkedBlockingQueue<>();
        private final List<long[]> offsetList;

        private boolean canStartFetchRow = false;

        AsyncDataHandler(List<ListenableFuture<SendResult<String, String>>> batchFutures,
            SliceResultSetSender sliceSender, List<long[]> offsetList) {
            this.batchFutures = batchFutures;
            this.sliceSender = sliceSender;
            this.offsetList = offsetList;
        }

        /**
         * start async data handler thread
         */
        public void start() {
            this.canStartFetchRow = true;
        }

        /**
         * add row to batch handler queue
         *
         * @param row row
         */
        public void addRow(Map<String, String> row) {
            this.batchData.add(row);
        }

        /**
         * wait queue empty to stop
         *
         * @throws InterruptedException InterruptedException
         */
        public void waitToStop() throws InterruptedException {
            while (!batchData.isEmpty()) {
                Thread.sleep(100);
            }
            this.canStartFetchRow = false;
        }

        @Override
        public void run() {
            log.info("start send slice row {}", slice.getName());
            while (canStartFetchRow) {
                if (Objects.isNull(batchData.peek())) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignore) {
                    }
                } else {
                    Map<String, String> value = batchData.poll();
                    batchFutures.add(sliceSender.resultSetTranslate(value, slice.getNo()));
                    if (batchFutures.size() == FETCH_SIZE) {
                        offsetList.add(getBatchFutureRecordOffsetScope(batchFutures));
                        batchFutures.clear();
                    }
                }
            }
            if (batchFutures.size() > 0) {
                offsetList.add(getBatchFutureRecordOffsetScope(batchFutures));
                batchFutures.clear();
            }
        }

        /**
         * check  sender is busy , if busy return true , else return false
         * batch queue size >= maxQueueSize return true , else return false
         *
         * @return boolean
         */
        public boolean isSenderBusy() {
            return batchData.size() >= maxQueueSize;
        }
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
