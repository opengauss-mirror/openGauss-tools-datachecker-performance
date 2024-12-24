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

import lombok.Getter;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
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
import java.util.List;
import java.util.Locale;
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
    private static final int MAX_RETRY_TIMES = 60;

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
            LogUtils.error(log, "{}table slice [{}] is error", ErrorCode.EXECUTE_SLICE_PROCESSOR,
                slice.toSimpleString(), ex);
        } finally {
            LogUtils.info(log, "table slice [{} count {}] is finally   ", slice.toSimpleString(), rowCount.get());
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
        int sliceCount = (int) slice.getRowCountOfInIds();
        if (slice.getRowCountOfInIds() == 0) {
            return;
        }
        QuerySqlEntry baseSliceSql = sliceStatement.buildSlice(tableMetadata, slice);
        List<String> pageStatementList = sliceStatement.buildPageStatement(baseSliceSql, sliceCount,
            slice.getFetchSize());
        SliceResultSetSender sliceSender = null;
        Connection connection = null;
        try {
            // 申请数据库链接
            long estimatedRowCount = Math.min(sliceCount, slice.getFetchSize());
            long estimatedMemorySize = estimatedMemorySize(tableMetadata.getAvgRowLength(), estimatedRowCount);
            connection = jdbcOperation.tryConnectionAndClosedAutoCommit(estimatedMemorySize);
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
            int idx = 0;
            for (String pageStatement : pageStatementList) {
                log.debug("executeSliceQueryStatementPage : {} : {}", ++idx, maskQuery(pageStatement));
                QueryParameters queryParameters = new QueryParameters(0, 0);
                if (isFirstStatement) {
                    // only use first page statement's start offset
                    startOffset = statementQuery(pageStatement, connection, sliceSender, asyncHandler, queryParameters);
                } else {
                    // other page statement's start offset is ignored
                    statementQuery(pageStatement, connection, sliceSender, asyncHandler, queryParameters);
                }
                isFirstStatement = false;
            }
            log.info("executeSliceQueryStatementPage : {} execute statement end", slice.getName());
            sliceExtend.setStartOffset(startOffset);
            waitToStopAsyncHandlerAndResources(asyncHandler);
            updateExtendSliceOffsetAndCount(sliceExtend, rowCount.get(), offsetList);
            log.info("executeSliceQueryStatementPage : {} async send end", slice.getName());
        } catch (Exception ex) {
            LogUtils.error(log, "{}slice [{}] has exception :",ErrorCode.EXECUTE_SLICE_QUERY, slice.getName(), ex);
            throw new ExtractDataAccessException(ex.getMessage());
        } finally {
            if (sliceSender != null) {
                sliceSender.agentsClosed();
            }
            jdbcOperation.releaseConnection(connection);
        }
    }

    /**
     * only used by log print
     *
     * @param pageStatement sql statement
     * @return mask statement
     */
    private String maskQuery(String pageStatement) {
        String[] split = pageStatement.toLowerCase(Locale.ROOT).split(" from ");
        return "select * from " + split[1];
    }

    private long statementQuery(String pageStatement, Connection connection, SliceResultSetSender sliceSender,
        AsyncDataHandler asyncHandler, QueryParameters queryParameters) {
        long startOffset = -1L;
        ExecutionStage executionStage = ExecutionStage.PREPARE;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        int rsIdx = 0;
        try {
            executionStage = ExecutionStage.EXECUTE;
            ps = connection.prepareStatement(pageStatement);
            ps.setFetchSize(FETCH_SIZE);
            resultSet = ps.executeQuery();
            startOffset = sliceSender.checkOffsetEnd();
            ResultSetMetaData rsmd = resultSet.getMetaData();
            executionStage = ExecutionStage.FETCH;
            while (resultSet.next()) {
                if (rsIdx >= queryParameters.getResultSetIdx()) {
                    this.rowCount.incrementAndGet();
                    if (asyncHandler.isSenderBusy()) {
                        Thread.sleep(100);
                    }
                    asyncHandler.addRow(sliceSender.resultSet(rsmd, resultSet));
                }
                rsIdx++;
            }
            executionStage = ExecutionStage.CLOSE;
            // 数据发送到异步处理线程中，关闭ps与rs
            ConnectionMgr.close(null, ps, resultSet);
        } catch (Exception ex) {
            log.error("{}execute query {}  executionStage: {} error,retry cause : {}", ErrorCode.EXECUTE_SLICE_QUERY,
                slice.toSimpleString(), executionStage, ex.getMessage());
            if (Objects.equals(executionStage, ExecutionStage.CLOSE)) {
                ConnectionMgr.close(null, ps, resultSet);
            } else {
                ConnectionMgr.close(connection, ps, resultSet);
                if (queryParameters.getRetryTimes() <= MAX_RETRY_TIMES) {
                    connection = jdbcOperation.tryConnectionAndClosedAutoCommit(0);
                    ++queryParameters.retryTimes;
                    queryParameters.resultSetIdx = rsIdx;
                    startOffset = statementQuery(pageStatement, connection, sliceSender, asyncHandler, queryParameters);
                } else {
                    log.error("{}execute query {} retry {} times error ,cause by ", ErrorCode.EXECUTE_QUERY_SQL,
                        maskQuery(pageStatement), queryParameters.getRetryTimes(), ex);
                    throw new ExtractDataAccessException(
                        "execute query " + maskQuery(pageStatement) + " retry " + MAX_RETRY_TIMES
                            + " times error ,cause by " + ex.getMessage());
                }
            }
        }
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
    private void executeQueryStatement(QuerySqlEntry sqlEntry, TableMetadata tableMetadata, SliceExtend sliceExtend) {
        SliceResultSetSender sliceSender = null;
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        try {
            // 获取连接，准备查询分片数据： 并开启数据异步处理线程
            List<long[]> offsetList = new CopyOnWriteArrayList<>();
            List<ListenableFuture<SendResult<String, String>>> batchFutures = new CopyOnWriteArrayList<>();
            sliceSender = createSliceResultSetSender(tableMetadata);
            sliceSender.setRecordSendKey(slice.getName());
            AsyncDataHandler asyncHandler = new AsyncDataHandler(batchFutures, sliceSender, offsetList);
            asyncHandler.start();
            context.asyncSendSlice(asyncHandler);
            // 申请数据库链接
            long estimatedRowCount = slice.isSlice() ? slice.getFetchSize() : tableMetadata.getTableRows();
            long estimatedMemorySize = estimatedMemorySize(tableMetadata.getAvgRowLength(), estimatedRowCount);
            connection = jdbcOperation.tryConnectionAndClosedAutoCommit(estimatedMemorySize);
            // 开始查询数据，并将结果推送到异步处理线程中。
            QueryParameters parameters = new QueryParameters(0, 0);
            long startOffset = statementQuery(sqlEntry.getSql(), connection, sliceSender, asyncHandler, parameters);
            sliceExtend.setStartOffset(startOffset);
            // 等待分片查询处理完成，关闭数据库连接，并关闭异步数据处理线程 ，关闭ps与rs
            waitToStopAsyncHandlerAndResources(asyncHandler);
            updateExtendSliceOffsetAndCount(sliceExtend, rowCount.get(), offsetList);
        } catch (Exception ex) {
            LogUtils.error(log, "{}slice [{}] has exception :", ErrorCode.EXECUTE_SLICE_QUERY, slice.getName(), ex);
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
     * statement query parameters
     */
    @Getter
    class QueryParameters {
        private int retryTimes;
        private int resultSetIdx;

        /**
         * build statement query paramters
         *
         * @param retryTimes retry times
         * @param resultSetIdx result set idx
         */
        public QueryParameters(int retryTimes, int resultSetIdx) {
            this.retryTimes = retryTimes;
            this.resultSetIdx = resultSetIdx;
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

/**
 * execution stage
 */
enum ExecutionStage {
    /**
     * prepare
     */
    PREPARE,
    /**
     * execute
     */
    EXECUTE,
    /**
     * fetch
     */
    FETCH,
    /**
     * close
     */
    CLOSE
}