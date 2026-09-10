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

import static org.opengauss.datachecker.extract.slice.process.TableCollationFactory.getTableCollation;

import com.alibaba.druid.pool.DruidDataSource;

import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.StrUtil;
import lombok.Getter;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.kafka.common.KafkaException;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;
import org.opengauss.datachecker.common.exception.ExtractException;
import org.opengauss.datachecker.common.exception.SendTopicMessageException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.SpringUtil;
import org.opengauss.datachecker.common.util.SqlUtil;
import org.opengauss.datachecker.extract.resource.ConnectionMgr;
import org.opengauss.datachecker.extract.resource.JdbcDataOperations;
import org.opengauss.datachecker.extract.resource.ResourceManager;
import org.opengauss.datachecker.extract.slice.SliceProcessorContext;
import org.opengauss.datachecker.extract.slice.common.SliceResultSetSender;
import org.opengauss.datachecker.extract.task.sql.FullQueryStatement;
import org.opengauss.datachecker.extract.task.sql.QuerySqlEntry;
import org.opengauss.datachecker.extract.task.sql.SliceQueryStatement;
import org.opengauss.datachecker.extract.task.sql.UnionPrimarySliceQueryStatement;
import org.springframework.beans.BeansException;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * JdbcSliceProcessor
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/8
 * @since ：11
 */
public class JdbcSliceProcessor extends AbstractSliceProcessor {
    private static final Logger log = LogUtils.getLogger(JdbcSliceProcessor.class);
    private static final int MAX_RETRY_TIMES = 3;

    private final JdbcDataOperations jdbcOperation;
    private final AtomicInteger rowCount = new AtomicInteger(0);

    /**
     * translate collate utf8_general_ci to utf8mb4_general_ci
     */
    private final Function<String, String> translateUtf8GeneralCi = tableCollation -> {
        if (StrUtil.equalsIgnoreCase(tableCollation, "utf8_general_ci")) {
            tableCollation = "utf8mb4_general_ci";
        }
        return tableCollation;
    };

    private final SqlFieldMasker sqlFieldMasker = pageStatement -> {
        try {
            Statement stmt = CCJSqlParserUtil.parse(pageStatement);
            if (stmt instanceof Select selectStmt) {
                if (selectStmt.getSelectBody() instanceof PlainSelect plainSelect) {
                    FromItem fromItem = plainSelect.getFromItem();
                    if (fromItem != null) {
                        return "select * from " + fromItem.toString();
                    }
                }
            }
            return pageStatement;
        } catch (JSQLParserException e) {
            LogUtils.warn(log, "parse sql failed, origin sql:{} error:{}", pageStatement, e.getMessage());
            return pageStatement;
        }
    };

    /**
     * JdbcSliceProcessor
     *
     * @param slice slice
     * @param context context
     */
    public JdbcSliceProcessor(SliceVo slice, SliceProcessorContext context, DruidDataSource dataSource) {
        super(slice, context);
        this.jdbcOperation = context.getJdbcDataOperations();
    }

    @Override
    public void run() {
        LogUtils.info(log, "table slice [{}] is beginning to extract data", slice.toSimpleString());
        TableMetadata tableMetadata = context.getTableMetaData(table);
        SliceExtend sliceExtend = createSliceExtend(tableMetadata.getTableHash());
        try {
            resetSmallTableRowCount(tableMetadata);
            String tableCollation = tableMetadata.getTableCollation();
            DataBaseType dataBaseType = ConfigCache.getValue(ConfigConstants.DATA_BASE_TYPE, DataBaseType.class);
            if (StrUtil.isEmpty(tableCollation)) {
                refreshTableCollation(tableMetadata, dataBaseType);
            }
            if (tableMetadata.isUnionPrimary()) {
                Assert.isTrue(isSuiteUnionPrimary(dataBaseType),
                    "Union primary is not supported by current database type " + dataBaseType.getDescription());
                executeSliceQueryStatementPage(tableMetadata, sliceExtend);
            } else {
                QuerySqlEntry queryStatement = createQueryStatement(tableMetadata);
                LogUtils.debug(log, "table [{}] query statement :  {}", table,
                    sqlFieldMasker.mask(queryStatement.getSql()));
                executeQueryStatement(queryStatement, tableMetadata, sliceExtend);
            }
        } catch (ExtractException ex) {
            sliceExtend.setStatus(-1);
            LogUtils.error(log, "{}table slice [{}] is error", ErrorCode.EXECUTE_SLICE_PROCESSOR,
                slice.toSimpleString(), ex);
        } catch (OutOfMemoryError oom) {
            sliceExtend.setStatus(-1);
            LogUtils.error(log, "{}table slice [{}] is error", ErrorCode.OUT_OF_MEMORY_ERROR, slice.toSimpleString(),
                oom);
            throw oom;
        } finally {
            LogUtils.info(log, "table slice [{} count {}] is finally   ", slice.toSimpleString(), rowCount.get());
            feedbackStatus(sliceExtend);
            context.saveProcessing(slice);
        }
    }

    private void resetSmallTableRowCount(TableMetadata tableMetadata) {
        if (tableMetadata.getTableRows() < 100) {
            DataBaseType dataBaseType = ConfigCache.getValue(ConfigConstants.DATA_BASE_TYPE, DataBaseType.class);
            boolean isOgB = ConfigCache.getBooleanValue(ConfigConstants.OG_COMPATIBILITY_B);
            String maskSchema = SqlUtil.escape(slice.getSchema(), dataBaseType, isOgB);
            String maskTable = SqlUtil.escape(slice.getTable(), dataBaseType, isOgB);
            String countSql = "select count(*) count from " + maskSchema + "." + maskTable;
            log.debug("reset small table row count for table [{}] is {}", tableMetadata.getTableName(), countSql);
            // Take the connection before entering the try block: if acquiring fails (e.g. admission
            // timeout) it throws straight up and the finally below never runs,
            // avoiding "releasing a slot that was never taken" inflating the connection count
            // and shrinking usable concurrency
            final Connection connection = jdbcOperation.tryConnectionAndClosedAutoCommit(0);
            try (PreparedStatement ps = connection.prepareStatement(countSql);
                 ResultSet resultSet = ps.executeQuery()) {
                if (resultSet.next()) {
                    long count = resultSet.getLong("count");
                    tableMetadata.setTableRows(count);
                    slice.setRowCountOfInIds(count);
                }
            } catch (SQLException e) {
                log.error("query table[{}] count sql statument [{}] is not valid", this.table, countSql);
            } finally {
                // Return the ResourceManager connection slot so small-table count queries do not eat the quota
                jdbcOperation.releaseConnection(connection);
            }
        }
    }

    private void refreshTableCollation(TableMetadata tableMetadata, DataBaseType dataBaseType) {
        String collationSql = getTableCollation(dataBaseType);
        if (StrUtil.isEmpty(collationSql)) {
            return;
        }
        // Take the connection before entering the try block: if acquiring fails (e.g. admission timeout)
        // it throws straight up without running the finally, avoiding an unpaired slot release
        final Connection connection = jdbcOperation.tryConnectionAndClosedAutoCommit(1);
        try (PreparedStatement preparedStatement = connection.prepareStatement(collationSql)) {
            preparedStatement.setString(1, tableMetadata.getSchema());
            preparedStatement.setString(2, tableMetadata.getTableName());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    String tableCollation = resultSet.getString(1);
                    tableMetadata.setTableCollation(translateUtf8GeneralCi.apply(tableCollation));
                }
            }
        } catch (SQLException ex) {
            LogUtils.error(log, "refresh table collation failed with exp:", ex);
        } finally {
            jdbcOperation.releaseConnection(connection);
        }
    }

    private boolean isSuiteUnionPrimary(DataBaseType dataBaseType) {
        return Objects.equals(dataBaseType, DataBaseType.OG) || Objects.equals(dataBaseType, DataBaseType.MS)
            || Objects.equals(dataBaseType, DataBaseType.O) || Objects.equals(dataBaseType, DataBaseType.OGRAC);
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
        UnionPrimarySliceQueryStatement sliceStatement = context.createSlicePageQueryStatement();
        int sliceCount = (int) slice.getRowCountOfInIds();
        if (slice.getRowCountOfInIds() == 0) {
            return;
        }
        QuerySqlEntry baseSliceSql = sliceStatement.buildSlice(tableMetadata, slice);
        List<String> pageStatementList = sliceStatement.buildPageStatement(baseSliceSql, sliceCount,
            slice.getFetchSize(), tableMetadata);
        LogUtils.debug(log, "table [{}] page query statement count: {}, first: {}",
                table, pageStatementList.size(), sqlFieldMasker.mask(pageStatementList.get(0)));
        SliceResultSetSender sliceSender = null;
        Connection connection = null;
        AsyncDataHandler asyncHandler = null;
        try {
            connection = jdbcOperation.tryConnectionAndClosedAutoCommit(MEMORY_GATE_TRIGGER);
            sliceSender = createSliceResultSetSender(tableMetadata);
            sliceSender.setRecordSendKey(slice.getName());
            List<long[]> offsetList = new CopyOnWriteArrayList<>();
            List<CompletableFuture<SendResult<String, String>>> batchFutures = new CopyOnWriteArrayList<>();
            asyncHandler = new AsyncDataHandler(batchFutures, sliceSender, offsetList);
            asyncHandler.start();
            context.asyncSendSlice(asyncHandler);
            StatementQueryResult pageResult = null;
            try {
                pageResult = executePagedStatements(pageStatementList, connection, sliceSender, asyncHandler);
            } finally {
                if (pageResult == null) {
                    // When executePagedStatements throws, its internal connection was already returned
                    // (released on retry / retries exhausted / reacquire failed); null it out so the
                    // finally cleanResource does not double-release the slot
                    connection = null;
                }
            }
            connection = pageResult.getConnection();
            log.debug("executeSliceQueryStatementPage : {} execute statement end", slice.getName());
            sliceExtend.setStartOffset(pageResult.getStartOffset());
            asyncHandler.waitToStop(false);
            updateExtendSliceOffsetAndCount(sliceExtend, rowCount.get(), offsetList);
            log.info("executeSliceQueryStatementPage : {} async send end", slice.getName());
        } catch (Exception ex) {
            LogUtils.error(log, "{}slice [{}] has exception :", ErrorCode.EXECUTE_SLICE_QUERY, slice.getName(), ex);
            throw new ExtractDataAccessException(ex.getMessage());
        } finally {
            cleanResource(sliceSender, asyncHandler, connection);
        }
    }

    /**
     * Execute the slice query statements page by page; returns the first page's start offset and
     * the finally used connection (retries may swap the connection).
     *
     * @param pageStatementList paged SQL statement list
     * @param connection database connection used by the first page
     * @param sliceSender slice data sender
     * @param asyncHandler async sending thread
     * @return start offset of the first page plus the final connection
     */
    private StatementQueryResult executePagedStatements(List<String> pageStatementList, Connection connection,
        SliceResultSetSender sliceSender, AsyncDataHandler asyncHandler) {
        Connection currentConnection = connection;
        boolean isFirstStatement = true;
        long startOffset = 0L;
        int idx = 0;
        for (String pageStatement : pageStatementList) {
            log.debug("executeSliceQueryStatementPage : {} : {}", ++idx, sqlFieldMasker.mask(pageStatement));
            QueryParameters queryParameters = new QueryParameters(0, 0);
            StatementQueryResult queryResult = null;
            try {
                queryResult = statementQuery(pageStatement, currentConnection, sliceSender, asyncHandler,
                    queryParameters);
            } finally {
                if (queryResult == null) {
                    // When statementQuery throws, its internal connection was already returned
                    // (released on retry / retries exhausted / reacquire failed); null it out
                    // so the finally does not double-release the slot
                    currentConnection = null;
                }
            }
            // A retry may have swapped the connection; later pages must use the latest one
            currentConnection = queryResult.getConnection();
            if (isFirstStatement) {
                startOffset = queryResult.getStartOffset();
            }
            isFirstStatement = false;
        }
        return new StatementQueryResult(startOffset, currentConnection);
    }

    private void cleanResource(SliceResultSetSender sliceSender, AsyncDataHandler asyncHandler, Connection connection) {
        if (sliceSender != null) {
            sliceSender.agentsClosed();
        }
        if (asyncHandler != null) {
            asyncHandler.waitToStop(true);
        }
        if (connection != null) {
            jdbcOperation.releaseConnection(connection);
        }
    }

    private StatementQueryResult statementQuery(String pageStatement, Connection connection,
        SliceResultSetSender sliceSender, AsyncDataHandler asyncHandler, QueryParameters queryParameters) {
        long startOffset = -1L;
        int rsIdx = 0;
        // Retries swap the connection; later rounds and the return value all use this local
        // variable, leaving the parameter untouched
        Connection currentConnection = connection;
        while (true) {
            ExecutionStage executionStage = ExecutionStage.PREPARE;
            PreparedStatement ps = null;
            ResultSet resultSet = null;
            try {
                executionStage = ExecutionStage.EXECUTE;
                ps = currentConnection.prepareStatement(pageStatement);
                // Clamp the fetch size adaptively against the table's row width, remaining memory,
                // and concurrency pressure from other admitted slices; keeps the Oracle BufferCache
                // from allocating a fetchSize-row array inside executeQuery() and OOM-ing directly.
                // Row width comes from TableMetadata.avgRowLength, or a conservative 4KB/row when missing
                TableMetadata md = context.getTableMetaData(table);
                int safeFetch = adaptFetchSizeToRemainingMemory(
                    md != null ? md.getAvgRowLength() : 4096L);
                ps.setFetchSize(safeFetch);
                resultSet = ps.executeQuery();
                startOffset = sliceSender.checkOffsetEnd();
                ResultSetMetaData rsmd = resultSet.getMetaData();
                executionStage = ExecutionStage.FETCH;
                while (resultSet.next()) {
                    if (rsIdx >= queryParameters.getResultSetIdx()) {
                        // Backpressure is guaranteed by the bounded queue put() in addRow;
                        // no predictive sleep needed here
                        asyncHandler.addRow(sliceSender.resultSet(rsmd, resultSet));
                        // Count only after the row is successfully enqueued: if row conversion throws and triggers
                        // a retry, that row will be re-sent; counting first would register one more row than
                        // actually sent, and the check side would under-pull and flag a failure
                        this.rowCount.incrementAndGet();
                    }
                    rsIdx++;
                }
                executionStage = ExecutionStage.CLOSE;
                // Rows already handed to the async processing thread; close ps and rs
                ConnectionMgr.close(null, ps, resultSet);
                return new StatementQueryResult(startOffset, currentConnection);
            } catch (SQLException | ExtractDataAccessException | KafkaException ex) {
                log.error("{}execute query {}  executionStage: {} error,retry cause : ", ErrorCode.EXECUTE_SLICE_QUERY,
                    slice.toSimpleString(), executionStage, ex);
                if (Objects.equals(executionStage, ExecutionStage.CLOSE)) {
                    ConnectionMgr.close(null, ps, resultSet);
                    return new StatementQueryResult(startOffset, currentConnection);
                }
                // Release the current connection and resource counters, then acquire a new connection
                // for the retry, so the retry path does not leak connections
                jdbcOperation.releaseConnection(currentConnection, ps, resultSet);
                // Mark as returned: if reacquiring below throws, the outer finally skips the
                // release based on this, avoiding a double slot release
                currentConnection = null;
                if (queryParameters.getRetryTimes() <= MAX_RETRY_TIMES) {
                    ++queryParameters.retryTimes;
                    queryParameters.resultSetIdx = rsIdx;
                    currentConnection = jdbcOperation.tryConnectionAndClosedAutoCommit(0);
                } else {
                    log.error("{}execute query {} retry {} times error ,cause by ", ErrorCode.EXECUTE_QUERY_SQL,
                        sqlFieldMasker.mask(pageStatement), queryParameters.getRetryTimes(), ex);
                    throw new ExtractDataAccessException(
                        "execute query " + sqlFieldMasker.mask(pageStatement) + " retry " + MAX_RETRY_TIMES
                            + " times error ,cause by " + ex.getMessage());
                }
            }
        }
    }

    private void executeQueryStatement(QuerySqlEntry sqlEntry, TableMetadata tableMetadata, SliceExtend sliceExtend) {
        SliceResultSetSender sliceSender = null;
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        AsyncDataHandler asyncHandler = null;
        try {
            // 获取连接，准备查询分片数据： 并开启数据异步处理线程
            List<long[]> offsetList = new CopyOnWriteArrayList<>();
            List<CompletableFuture<SendResult<String, String>>> batchFutures = new CopyOnWriteArrayList<>();
            sliceSender = createSliceResultSetSender(tableMetadata);
            sliceSender.setRecordSendKey(slice.getName());
            asyncHandler = new AsyncDataHandler(batchFutures, sliceSender, offsetList);
            asyncHandler.start();
            context.asyncSendSlice(asyncHandler);
            // 申请数据库链接
            connection = jdbcOperation.tryConnectionAndClosedAutoCommit(MEMORY_GATE_TRIGGER);
            // 开始查询数据，并将结果推送到异步处理线程中。
            QueryParameters parameters = new QueryParameters(0, 0);
            StatementQueryResult queryResult = null;
            try {
                queryResult = statementQuery(sqlEntry.getSql(), connection, sliceSender, asyncHandler, parameters);
            } finally {
                if (queryResult == null) {
                    // When statementQuery throws, its internal connection was already returned
                    // (released on retry / retries exhausted / reacquire failed); null it out
                    // so the finally does not double-release the slot
                    connection = null;
                }
            }
            // A retry may have swapped the connection; resource release must use the latest one
            connection = queryResult.getConnection();
            long startOffset = queryResult.getStartOffset();
            sliceExtend.setStartOffset(startOffset);
            // 等待分片查询处理完成，关闭数据库连接，并关闭异步数据处理线程 ，关闭ps与rs
            asyncHandler.waitToStop(false);
            updateExtendSliceOffsetAndCount(sliceExtend, rowCount.get(), offsetList);
        } catch (Exception ex) {
            LogUtils.error(log, "{}slice [{}] has exception :", ErrorCode.EXECUTE_SLICE_QUERY, slice.getName(), ex);
            throw new ExtractDataAccessException(ex.getMessage());
        } finally {
            ConnectionMgr.close(connection, ps, resultSet);
            if (sliceSender != null) {
                sliceSender.agentsClosed();
            }
            if (asyncHandler != null) {
                asyncHandler.waitToStop(true);
            }
            if (connection != null) {
                jdbcOperation.releaseConnection(connection);
            }
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
     * statement query result: carries the query start offset and the currently used connection (a retry may swap it)
     */
    @Getter
    static class StatementQueryResult {
        private final long startOffset;
        private final Connection connection;

        StatementQueryResult(long startOffset, Connection connection) {
            this.startOffset = startOffset;
            this.connection = connection;
        }
    }

    /**
     * async data handler thread
     */
    class AsyncDataHandler implements Runnable {
        private static final int DRAIN_BATCH_SIZE = 1000;
        private static final int OFFER_TIMEOUT_SECONDS = 60;

        private final List<CompletableFuture<SendResult<String, String>>> batchFutures;
        private final SliceResultSetSender sliceSender;
        private final int maxQueueSize = 10000;

        // Bounded queue: caps the memory held by pending data; when full, addRow waits with a timeout
        // and throws on timeout so the slice fails and gets retried
        private final BlockingQueue<Map<String, String>> batchData = new LinkedBlockingQueue<>(maxQueueSize);
        private final List<long[]> offsetList;

        // Send completion counting: submitted rows vs kafka-acked rows; equal means all of
        // this slice's data is written to the topic
        private final AtomicLong submittedCount = new AtomicLong(0);
        private final AtomicLong completedCount = new AtomicLong(0);

        // Lower/upper bounds of kafka offsets acked for this slice, used to restore the slice's
        // offset range registration in offsetList
        private final AtomicLong minOffset = new AtomicLong(Long.MAX_VALUE);
        private final AtomicLong maxOffset = new AtomicLong(Long.MIN_VALUE);

        /**
         * Send throttling semaphore: the sending thread acquires one permit per row,
         * the Kafka ack callback releases it.
         * When Kafka is slow the sending thread blocks first (not holding a database connection),
         * slowing down database reads in turn
         */
        private final Semaphore inFlightLimit = new Semaphore(fetchSize);

        private volatile boolean canStartFetchRow = false;

        AsyncDataHandler(List<CompletableFuture<SendResult<String, String>>> batchFutures,
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
            submittedCount.incrementAndGet();
            try {
                // When the queue is full, wait with a timeout; on timeout throw so the slice
                // fails and the connection is released.
                // With the semaphore throttle working normally the queue is rarely full; this timeout is only
                // a last-resort connection guard for extreme cases
                if (!this.batchData.offer(row, OFFER_TIMEOUT_SECONDS, java.util.concurrent.TimeUnit.SECONDS)) {
                    submittedCount.decrementAndGet();
                    // A full queue usually comes with the heap pushed up by pending data: throw so this
                    // slice fails and retries, and temporarily tighten new slice admission,
                    // keeping the retry plus existing concurrency from pushing the heap into OOM
                    jdbcOperation.tightenAdmissionOnBackpressure();
                    throw new ExtractDataAccessException(
                        "slice " + slice.getName() + " queue full after " + OFFER_TIMEOUT_SECONDS
                            + "s, kafka backpressure");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                submittedCount.decrementAndGet();
                throw new ExtractDataAccessException(
                    "slice " + slice.getName() + " addRow interrupted on queue offer");
            }
        }

        /**
         * wait all submitted rows send completed (kafka ack) to stop
         *
         * @param isForceClose is force close
         */
        public void waitToStop(boolean isForceClose) {
            // Overall timeout guard: under extreme anomalies the submitted and completed counts may
            // stay unequal for a long time; give up waiting after 5 minutes so the slice thread never blocks forever
            final long maxWaitMs = 5 * 60 * 1000L;
            final long startMs = System.currentTimeMillis();
            long lastLoggedMs = 0L;
            while (submittedCount.get() != completedCount.get()) {
                long elapsed = System.currentTimeMillis() - startMs;
                if (elapsed > maxWaitMs && !isForceClose) {
                    long diff = submittedCount.get() - completedCount.get();
                    LogUtils.error(log,
                        "" + ErrorCode.EXECUTE_SLICE_QUERY + " slice [" + slice.getName()
                        + "] waitToStop timeout after " + elapsed + "ms, submitted-completed="
                        + diff + " remaining. Force break to avoid thread hanging.");
                    break;
                }
                ThreadUtil.sleep(10L);
                if (isForceClose) {
                    break;
                }
                if (elapsed - lastLoggedMs > 30_000) {
                    lastLoggedMs = elapsed;
                    LogUtils.info(log,
                        "waitToStop " + slice.getName() + " still waiting: submitted="
                        + submittedCount.get() + " completed=" + completedCount.get() + " diff="
                        + (submittedCount.get() - completedCount.get()) + " (" + elapsed + "ms)");
                }
            }
            this.canStartFetchRow = false;

            int remainingRows = batchData.size();
            if (remainingRows > 0) {
                long newSubmitted = submittedCount.addAndGet(-remainingRows);
                LogUtils.info(log,
                    "waitToStop " + slice.getName() + " drainRemainingFromQueue: rollback "
                    + remainingRows + " un-sent rows from batchData, submitted now "
                    + newSubmitted + " (completed " + completedCount.get() + ")");
                batchData.clear();
            } else {
                batchData.clear();
            }
            this.batchFutures.clear();
            this.offsetList.clear();
            // Restore the slice's offset range registration: the check side seeks to startOffset
            // and then pulls, filtering by key
            if (maxOffset.get() >= 0) {
                this.offsetList.add(new long[] {minOffset.get(), maxOffset.get()});
            }
        }

        @Override
        public void run() {
            log.info("start send slice row {}", slice.getName());
            final List<Map<String, String>> drainBuffer = new ArrayList<>(DRAIN_BATCH_SIZE);
            while (canStartFetchRow) {
                // Drain the queue in batches to reduce per-row poll lock contention
                drainBuffer.clear();
                batchData.drainTo(drainBuffer, DRAIN_BATCH_SIZE);
                if (drainBuffer.isEmpty()) {
                    ThreadUtil.sleep(10L);
                    continue;
                }
                for (Map<String, String> value : drainBuffer) {
                    boolean isAcquired = false;
                    while (canStartFetchRow && !isAcquired) {
                        try {
                            isAcquired = inFlightLimit.tryAcquire(1, java.util.concurrent.TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            this.canStartFetchRow = false;
                        }
                    }
                    if (!isAcquired) {
                        submittedCount.decrementAndGet();
                        continue;
                    }
                    try {
                        sliceSender.resultSetTranslate(value, slice.getNo())
                                    .whenComplete((result, ex) -> {
                                        inFlightLimit.release();
                                        completedCount.incrementAndGet();
                                        if (result != null) {
                                            long offset = result.getRecordMetadata().offset();
                                            minOffset.accumulateAndGet(offset, Math::min);
                                            maxOffset.accumulateAndGet(offset, Math::max);
                                        }
                                    });
                    } catch (SendTopicMessageException | NullPointerException ex) {
                        inFlightLimit.release();
                        completedCount.incrementAndGet();
                        LogUtils.error(log, "{}slice [{}] send row error :", ErrorCode.EXECUTE_SLICE_QUERY,
                            slice.getName(), ex);
                    }
                }
            }
        }
    }

    private SliceResultSetSender createSliceResultSetSender(TableMetadata tableMetadata) {
        return new SliceResultSetSender(tableMetadata,
            context.createSliceFixedKafkaAgents(topic, slice.getName(), slice.getPtn()));
    }

    private void updateExtendSliceOffsetAndCount(SliceExtend sliceExtend, int rowCount, List<long[]> offsetList) {
        sliceExtend.setStartOffset(getMinOffset(offsetList));
        sliceExtend.setEndOffset(getMaxOffset(offsetList));
        sliceExtend.setCount(rowCount);
    }

    /**
     * Compute a safe fetch size adaptively from currently available memory and row width, controlling
     * the transient array allocation the Oracle JDBC BufferCache performs inside executeQuery() at the
     * source, to avoid an outright OOM with 100+ concurrent slices.
     * Rules:
     *   1. Cap by the process's "remaining assignable memory": maxHeap - usedHeap - a 25% safety margin
     *   2. Divide by the "current remaining connection quota" (slots still open for admission) as the
     *      concurrency factor: per-slot budget = assignable / remaining, so filling every remaining slot
     *      stays within assignable; assignable itself shrinks as usedHeap grows, providing negative feedback
     *   3. Clamp the result to [FETCH_SIZE_MIN, fetchSize (the member's configured default)]
     *
     * @param avgRowLength estimated bytes per row (DB metadata avg_row_length, or 4KB when absent)
     * @return a safe fetch size
     */
    private int adaptFetchSizeToRemainingMemory(long avgRowLength) {
        Runtime rt = Runtime.getRuntime();
        long maxHeap = rt.maxMemory();
        long usedHeap = rt.totalMemory() - rt.freeMemory();
        long headroom = maxHeap / 4;
        long assignable = Math.max(1L, maxHeap - usedHeap - headroom);
        // Divisor is the CURRENT remaining connection quota (ResourceManager.maxConnectionCount(), i.e. the
        // slots still open for admission), not the max-active cap: per-slot budget = assignable / remaining,
        // so filling every remaining slot stays within assignable, and usedHeap feedback tightens it further.
        int activeUpperBound = 100;
        try {
            ResourceManager rm = SpringUtil.getBean(ResourceManager.class);
            if (rm != null) {
                activeUpperBound = Math.max(1, rm.maxConnectionCount());
            }
        } catch (BeansException ignore) {
            // Fallback for an extreme startup race; does not affect the main flow
        }
        int finalActiveUpperBound = Math.max(1, activeUpperBound);
        long perConnBudget = assignable / finalActiveUpperBound;
        // perConnBudget must at least fit fetchSize rows x 2 copies (driver + in-flight)
        long rowLen = Math.max(1L, avgRowLength);
        long maxRows = perConnBudget / Math.max(1L, rowLen * 2L);
        int clamped;
        if (maxRows <= 0) {
            clamped = FETCH_SIZE_MIN;
        } else if (maxRows >= fetchSize) {
            clamped = fetchSize;
        } else {
            clamped = (int) Math.max(FETCH_SIZE_MIN, maxRows);
        }
        if (clamped < fetchSize) {
            log.info(
                "adaptFetchSize compressed table={} avgRowLen={} : fetchSize {} -> {} "
                    + " (assignable={} bytes, upperBound={})",
                table, rowLen, fetchSize, clamped, assignable, activeUpperBound);
        }
        return clamped;
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

class TableCollationFactory {
    private static final Map<DataBaseType, String> COLLATION = new HashMap<>();

    static {
        COLLATION.put(DataBaseType.OG, "select distinct collation_name from information_schema.columns "
            + "where table_schema=? and table_name=? and collation_name is not null limit 1");
        COLLATION.put(DataBaseType.MS,
            "select table_collation from information_schema.tables where table_schema = ? and table_name = ?");
        COLLATION.put(DataBaseType.O, "");
        COLLATION.put(DataBaseType.OGRAC, "");
    }

    /**
     * get table collation
     *
     * @param dataBaseType data base type
     * @return table collation
     */
    public static String getTableCollation(DataBaseType dataBaseType) {
        return COLLATION.getOrDefault(dataBaseType, "");
    }
}

/**
 * mask query sql fields,this only used by log print
 */
@FunctionalInterface
interface SqlFieldMasker {
    /**
     * mask query sql fields
     *
     * @param statementSql query sql statement
     * @return mask query sql statement
     * @throws ExtractDataAccessException current sql is invalid
     */
    String mask(String statementSql) throws ExtractDataAccessException;
}