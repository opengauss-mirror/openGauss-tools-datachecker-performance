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

import java.util.concurrent.CompletableFuture;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
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
    private final DruidDataSource dataSource;

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
        String[] split = pageStatement.toLowerCase(Locale.ROOT).split(" from ");
        if (split.length != 2) {
            throw new ExtractDataAccessException("sql statement is not valid");
        }
        return "select * from " + split[1];
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
        this.dataSource = dataSource;
    }

    @Override
    public void run() {
        LogUtils.info(log, "table slice [{}] is beginning to extract data", slice.toSimpleString());
        TableMetadata tableMetadata = context.getTableMetaData(table);
        SliceExtend sliceExtend = createSliceExtend(tableMetadata.getTableHash());
        try {
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

    private void refreshTableCollation(TableMetadata tableMetadata, DataBaseType dataBaseType) {
        try (Connection connection = jdbcOperation.tryConnectionAndClosedAutoCommit(1);
            PreparedStatement preparedStatement = connection.prepareStatement(getTableCollation(dataBaseType))) {
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
        AsyncDataHandler asyncHandler = null;
        try {
            long estimatedRowCount = Math.min(sliceCount, slice.getFetchSize());
            long estimatedMemorySize = estimatedMemorySize(tableMetadata.getAvgRowLength(), estimatedRowCount);
            connection = jdbcOperation.tryConnectionAndClosedAutoCommit(estimatedMemorySize);
            sliceSender = createSliceResultSetSender(tableMetadata);
            sliceSender.setRecordSendKey(slice.getName());
            List<long[]> offsetList = new CopyOnWriteArrayList<>();
            List<CompletableFuture<SendResult<String, String>>> batchFutures = new CopyOnWriteArrayList<>();
            asyncHandler = new AsyncDataHandler(batchFutures, sliceSender, offsetList);
            asyncHandler.start();
            context.asyncSendSlice(asyncHandler);
            boolean isFirstStatement = true;
            long startOffset = 0L;
            int idx = 0;
            for (String pageStatement : pageStatementList) {
                log.debug("executeSliceQueryStatementPage : {} : {}", ++idx, sqlFieldMasker.mask(pageStatement));
                QueryParameters queryParameters = new QueryParameters(0, 0);
                if (isFirstStatement) {
                    startOffset = statementQuery(pageStatement, connection, sliceSender, asyncHandler, queryParameters);
                } else {
                    statementQuery(pageStatement, connection, sliceSender, asyncHandler, queryParameters);
                }
                isFirstStatement = false;
            }
            log.debug("executeSliceQueryStatementPage : {} execute statement end", slice.getName());
            sliceExtend.setStartOffset(startOffset);
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

    private void cleanResource(SliceResultSetSender sliceSender, AsyncDataHandler asyncHandler, Connection connection) {
        if (sliceSender != null) {
            sliceSender.agentsClosed();
        }
        if (asyncHandler != null) {
            asyncHandler.waitToStop(true);
        }
        jdbcOperation.releaseConnection(connection);
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
                        ThreadUtil.sleep(100);
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
                        sqlFieldMasker.mask(pageStatement), queryParameters.getRetryTimes(), ex);
                    throw new ExtractDataAccessException(
                        "execute query " + sqlFieldMasker.mask(pageStatement) + " retry " + MAX_RETRY_TIMES
                            + " times error ,cause by " + ex.getMessage());
                }
            }
        }
        return startOffset;
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
            long estimatedRowCount = slice.isSlice() ? slice.getFetchSize() : tableMetadata.getTableRows();
            long estimatedMemorySize = estimatedMemorySize(tableMetadata.getAvgRowLength(), estimatedRowCount);
            connection = jdbcOperation.tryConnectionAndClosedAutoCommit(estimatedMemorySize);
            // 开始查询数据，并将结果推送到异步处理线程中。
            QueryParameters parameters = new QueryParameters(0, 0);
            long startOffset = statementQuery(sqlEntry.getSql(), connection, sliceSender, asyncHandler, parameters);
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
        private final List<CompletableFuture<SendResult<String, String>>> batchFutures;
        private final SliceResultSetSender sliceSender;
        private final int maxQueueSize = 10000;
        private final BlockingQueue<Map<String, String>> batchData = new LinkedBlockingQueue<>();
        private final List<long[]> offsetList;

        private boolean canStartFetchRow = false;

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
            this.batchData.add(row);
        }

        /**
         * wait queue empty to stop
         *
         * @param isForceClose is force close
         */
        public void waitToStop(boolean isForceClose) {
            while (!batchData.isEmpty()) {
                ThreadUtil.sleep(100);
                if (isForceClose) {
                    break;
                }
            }
            this.canStartFetchRow = false;
            this.batchData.clear();
            this.batchFutures.clear();
            this.offsetList.clear();
        }

        @Override
        public void run() {
            log.info("start send slice row {}", slice.getName());
            while (canStartFetchRow) {
                if (Objects.isNull(batchData.peek())) {
                    ThreadUtil.sleep(100);
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

class TableCollationFactory {
    private static final Map<DataBaseType, String> COLLATION = new HashMap<>();

    static {
        COLLATION.put(DataBaseType.OG, "select distinct collation_name from information_schema.columns "
            + "where table_schema=? and table_name=? and collation_name is not null limit 1");
        COLLATION.put(DataBaseType.MS,
            "select table_collation from information_schema.tables where table_schema = ? and table_name = ?");
        COLLATION.put(DataBaseType.O,
            "SELECT DEFAULT_COLLATION FROM ALL_TAB_COLUMNS WHERE OWNER = ? AND TABLE_NAME = ?;");
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