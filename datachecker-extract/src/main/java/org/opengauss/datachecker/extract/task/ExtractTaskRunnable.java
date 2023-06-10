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

package org.opengauss.datachecker.extract.task;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.opengauss.datachecker.common.constant.DynamicTpConstant;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.exception.CreateTopicTimeOutException;
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;
import org.opengauss.datachecker.common.exception.ExtractException;
import org.opengauss.datachecker.common.service.DynamicThreadPoolManager;
import org.opengauss.datachecker.common.util.TaskUtil;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.cache.TopicCache;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.kafka.KafkaAdminService;
import org.opengauss.datachecker.extract.resource.ResourceManager;
import org.opengauss.datachecker.extract.task.sql.QuerySqlEntry;
import org.opengauss.datachecker.extract.task.sql.SelectSqlBuilder;
import org.opengauss.datachecker.extract.util.MetaDataUtil;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.NonNull;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.opengauss.datachecker.common.constant.DynamicTpConstant.TABLE_PARALLEL_EXECUTOR;

/**
 * Data extraction thread class
 *
 * @author wang chao
 * @date 2022/5/12 19:17
 * @since 11
 **/
@Slf4j
public class ExtractTaskRunnable implements Runnable {
    private static final int FETCH_SIZE = 10000;
    private static final int TABLE_ROW_ESTIMATE_COUNT = 10000000;

    private final ExtractTask task;
    private final Endpoint endpoint;
    private final DataBaseType databaseType;
    private final String schema;
    private final DynamicThreadPoolManager dynamicThreadPoolManager;
    private final CheckingFeignClient checkingFeignClient;
    private final JdbcDataExtractionOperations jdbcOperation;
    private final KafkaOperations kafkaOperate;

    /**
     * Thread Constructor
     *
     * @param processNo processNo
     * @param task      task information
     * @param support   Thread helper class
     */
    public ExtractTaskRunnable(String processNo, ExtractTask task, ExtractThreadSupport support) {
        this.task = task;
        this.databaseType = support.getExtractProperties().getDatabaseType();
        this.schema = support.getExtractProperties().getSchema();
        this.endpoint = support.getExtractProperties().getEndpoint();
        this.jdbcOperation = new JdbcDataExtractionOperations(support.getDataSource(), support.getResourceManager());
        this.checkingFeignClient = support.getCheckingFeignClient();
        this.dynamicThreadPoolManager = support.getDynamicThreadPoolManager();
        this.kafkaOperate = new KafkaOperations(support.getKafkaTemplate(), support.getKafkaAdminService());
        kafkaOperate.init(processNo, endpoint);
    }

    @Override
    public void run() {
        try {
            log.debug("table {} extract begin", task.getTableName());
            TableMetadata tableMetadata = task.getTableMetadata();
            kafkaOperate.createTopic(task.getTopic());
            log.debug("table {} extract create topic", task.getTableName());
            QueryTableRowContext context = new QueryTableRowContext(tableMetadata, databaseType, kafkaOperate);
            final int[][] taskOffset = TaskUtil.calcAutoTaskOffset(tableMetadata.getTableRows());
            if (isNotSlice(tableMetadata, taskOffset)) {
                executeTask(context);
            } else {
                executeMultiTaskOffset(taskOffset, context);
            }
            checkingFeignClient.refreshTableExtractStatus(task.getTableName(), endpoint, endpoint.getCode());
        } catch (Exception ex) {
            checkingFeignClient.refreshTableExtractStatus(task.getTableName(), endpoint, -1);
            log.error("extract", ex);
        }
    }

    private boolean isNotSlice(TableMetadata tableMetadata, int[][] taskOffset) {
        return tableMetadata.getTableRows() > TABLE_ROW_ESTIMATE_COUNT || taskOffset.length == 1
            || jdbcOperation.getParallelQueryDop() == 1;
    }

    private void executeMultiTaskOffset(int[][] taskOffset, QueryTableRowContext context) {
        if (taskOffset == null || taskOffset.length < 2) {
            return;
        }
        LocalDateTime start = LocalDateTime.now();
        try {
            Vector<Boolean> sliceQuantityAnalysis = parallelExtractTableData(taskOffset, context);
            log.debug("table=[{}] parallel task completed, cost {} milliseconds", context.getTableName(),
                Duration.between(start, LocalDateTime.now()).toMillis());
            // any slice query count small current slice offset ,then this table had queried all table rows.
            if (sliceQuantityAnalysis.stream().anyMatch((noEqual -> !noEqual))) {
                return;
            }
            fixedParallelExtractTableData(taskOffset, context);
        } catch (Exception ex) {
            log.error("jdbc parallel query has unknown error [{}] : ", context.getTableName(), ex);
            throw new ExtractDataAccessException();
        } finally {
            log.info("table=[{}] execution completed, taking a total of {} milliseconds", context.getTableName(),
                Duration.between(start, LocalDateTime.now()).toMillis());
        }
    }

    private void fixedParallelExtractTableData(int[][] taskOffset, QueryTableRowContext context) {
        // Fix inaccurate statistics of the total number of table row records
        final SelectSqlBuilder sqlBuilder = new SelectSqlBuilder(context.getTableMetadata(), schema);
        long fixOffset = Arrays.stream(taskOffset).mapToInt(offset -> offset[1]).max().getAsInt();
        long fixStart = Arrays.stream(taskOffset).mapToInt(offset -> offset[0] + offset[1]).max().getAsInt();
        AtomicLong queryRowSize = new AtomicLong(fixOffset);
        while (queryRowSize.get() == fixOffset) {
            final LocalDateTime fixStartTime = LocalDateTime.now();
            final String fixQuerySql =
                sqlBuilder.dataBaseType(databaseType).isDivisions(true).offset(fixStart, fixOffset).builder();
            Connection connection = null;
            try {
                long freeMemory = evaluateSize(context.getAvgRowLength(), fixOffset);
                connection = jdbcOperation.tryConnectionAndClosedAutoCommit(freeMemory);
                log.debug("fixQuerySql table[{}] sql: [{}]", context.getTableName(), fixQuerySql);
                int rowCount = jdbcOperation.resultSetHandler(connection, fixQuerySql, context, (int) fixOffset);
                queryRowSize.set(rowCount);
                fixStart = fixStart + fixOffset;
            } catch (SQLException ex) {
                log.error("jdbc query stream [{}] error : {}", fixQuerySql, ex.getMessage());
                throw new ExtractDataAccessException();
            } finally {
                jdbcOperation.releaseConnection(connection);
                log.debug("fixed query [{}] cost [{}] milliseconds", context.getTableName(),
                    Duration.between(fixStartTime, LocalDateTime.now()).toMillis());
            }
        }
    }

    private Vector<Boolean> parallelExtractTableData(int[][] taskOffset, QueryTableRowContext context)
        throws SQLException, InterruptedException {
        ThreadPoolExecutor executor = dynamicThreadPoolManager.getExecutor(TABLE_PARALLEL_EXECUTOR);
        AtomicInteger exceptionCount = new AtomicInteger(0);
        CountDownLatch countDownLatch = new CountDownLatch(taskOffset.length);
        List<QuerySqlEntry> querySqlList = new ArrayList<>();
        Vector<Boolean> sliceQuantityAnalysis = new Vector<>();
        enableParallelQueryDop(taskOffset);
        builderQuerySqlByTaskOffset(taskOffset, context.getTableMetadata(), querySqlList);
        int fixedOffset = Arrays.stream(taskOffset).mapToInt(offset -> offset[1]).max().getAsInt();
        querySqlList.forEach(queryEntry -> {
            executor.submit(() -> {
                Connection connection = null;
                try {
                    long freeMemory = evaluateSize(context.getAvgRowLength(), fixedOffset);
                    connection = jdbcOperation.tryConnectionAndClosedAutoCommit(freeMemory);
                    String sliceSql = queryEntry.getSql();
                    log.debug("table=[{}] jdbc parallel query {}", context.getTableName(), sliceSql);
                    int rowCount =
                        jdbcOperation.resultSetHandlerParallelContext(connection, sliceSql, context, FETCH_SIZE);
                    sliceQuantityAnalysis.add(rowCount == queryEntry.getOffset());
                } catch (SQLException ex) {
                    exceptionCount.incrementAndGet();
                    log.error("jdbc parallel query [{}] error : {}", queryEntry.getSql(), ex.getMessage());
                    throw new ExtractDataAccessException();
                } finally {
                    countDown(context.getTableName(), countDownLatch, executor);
                    jdbcOperation.releaseConnection(connection);
                }
            });
        });
        countDownLatch.await();
        if (exceptionCount.get() > 0) {
            String msg =
                "Table " + context.getTableName() + " parallel query has " + exceptionCount.get() + " task exception";
            log.error(msg);
            throw new ExtractDataAccessException(msg);
        }
        return sliceQuantityAnalysis;
    }

    private void enableParallelQueryDop(int[][] taskOffset) throws SQLException {
        int dop = Math.min(taskOffset.length, jdbcOperation.getParallelQueryDop());
        jdbcOperation.enableDatabaseParallelQuery(dop);
    }

    private void countDown(String tableName, CountDownLatch countDownLatch, ThreadPoolExecutor executor) {
        countDownLatch.countDown();
        logNumberOfGlobalTasks(tableName, countDownLatch, executor);
    }

    private void logNumberOfGlobalTasks(String tableName, CountDownLatch countDownLatch, ThreadPoolExecutor executor) {
        if (Objects.nonNull(countDownLatch) && countDownLatch.getCount() > 0) {
            log.debug("parallel extract table [{}] remaining [{}] tasks ,tpExecutor core={} remain [{}, {},{}]",
                tableName, countDownLatch.getCount(), executor.getCorePoolSize(), executor.getCompletedTaskCount(),
                executor.getTaskCount() - executor.getCompletedTaskCount(), executor.getActiveCount());
        } else {
            log.debug("extract table [{}] tpExecutor core={}:{}  remain [{}, {},{}]", tableName,
                executor.getCorePoolSize(), executor.getLargestPoolSize(), executor.getCompletedTaskCount(),
                executor.getTaskCount() - executor.getCompletedTaskCount(), executor.getActiveCount());
        }
    }

    private void builderQuerySqlByTaskOffset(int[][] taskOffset, TableMetadata tableMetadata,
        List<QuerySqlEntry> querySqlList) {
        final int taskCount = taskOffset.length;
        final SelectSqlBuilder sqlBuilder = new SelectSqlBuilder(tableMetadata, schema);
        task.setDivisionsTotalNumber(taskCount);
        IntStream.range(0, taskCount).forEach(idx -> {
            final String querySql = sqlBuilder.dataBaseType(databaseType).isDivisions(task.isDivisions())
                                              .offset(taskOffset[idx][0], taskOffset[idx][1]).builder();
            querySqlList
                .add(new QuerySqlEntry(tableMetadata.getTableName(), querySql, taskOffset[idx][0], taskOffset[idx][1]));
        });
    }

    private void executeTask(QueryTableRowContext context) {
        final String tableName = context.getTableName();
        final LocalDateTime start = LocalDateTime.now();
        String queryAllRows = builderFullTableQueryStatement(context.getTableMetadata());
        Connection connection = null;
        int rowCount = 0;
        try {
            connection = jdbcOperation.tryConnectionAndClosedAutoCommit();
            log.debug("Table {} query {}", tableName, queryAllRows);
            rowCount = jdbcOperation.resultSetHandler(connection, queryAllRows, context, FETCH_SIZE);
        } catch (SQLException ex) {
            log.error("jdbc query  [{}] error : {}", queryAllRows, ex.getMessage());
            throw new ExtractDataAccessException();
        } finally {
            jdbcOperation.releaseConnection(connection);
            log.info("query table [{}] row-count [{}] cost [{}] milliseconds", tableName, rowCount,
                Duration.between(start, LocalDateTime.now()).toMillis());
            logNumberOfGlobalTasks(tableName, null,
                dynamicThreadPoolManager.getExecutor(DynamicTpConstant.EXTRACT_EXECUTOR));
        }
    }

    private String builderFullTableQueryStatement(TableMetadata tableMetadata) {
        final SelectSqlBuilder sqlBuilder = new SelectSqlBuilder(tableMetadata, schema);
        return sqlBuilder.dataBaseType(databaseType).isDivisions(false).builder();
    }

    private long evaluateSize(long avgRowLength, long rows) {
        return ((avgRowLength + 30) * Math.min(rows, FETCH_SIZE));
    }

    /**
     * jdbc data extraction operations
     */
    class JdbcDataExtractionOperations {
        private static final String OPEN_GAUSS_PARALLEL_QUERY = "set query_dop to %s;";

        private final DataSource jdbcDataSource;
        private final ResourceManager resourceManager;

        /**
         * constructor
         *
         * @param jdbcDataSource  datasource
         * @param resourceManager resourceManager
         */
        public JdbcDataExtractionOperations(DataSource jdbcDataSource, ResourceManager resourceManager) {
            this.jdbcDataSource = jdbcDataSource;
            this.resourceManager = resourceManager;
        }

        /**
         * try to get a jdbc connection and close auto commit.
         *
         * @param allocMemory allocMemory
         * @return Connection
         * @throws SQLException SQLException
         */
        public Connection tryConnectionAndClosedAutoCommit(long allocMemory) throws SQLException {
            takeConnection(allocMemory);
            return getConnectionAndClosedAutoCommit();
        }

        /**
         * try to get a jdbc connection and close auto commit.
         *
         * @return Connection
         * @throws SQLException SQLException
         */
        public Connection tryConnectionAndClosedAutoCommit() throws SQLException {
            takeConnection();
            return getConnectionAndClosedAutoCommit();
        }

        private Connection getConnectionAndClosedAutoCommit() throws SQLException {
            if (isShutdown()) {
                String message = "extract service is shutdown ,task of table is canceled!";
                throw new ExtractDataAccessException(message);
            }
            Connection connection = jdbcDataSource.getConnection();
            if (connection.getAutoCommit()) {
                connection.setAutoCommit(false);
            }
            return connection;
        }

        /**
         * release connection
         *
         * @param connection connection
         */
        public void releaseConnection(Connection connection) {
            resourceManager.release();
            DataSourceUtils.releaseConnection(connection, jdbcDataSource);
        }

        /**
         * use a jdbc connection to query sql ,and parse and hash query result.then hash result send kafka topic
         *
         * @param connection connection
         * @param sql        sql
         * @param context    context
         * @param fetchSize  fetchSize
         * @return resultSize
         * @throws SQLException SQLException
         */
        public int resultSetHandler(Connection connection, String sql, QueryTableRowContext context, int fetchSize)
            throws SQLException {
            LocalDateTime start = LocalDateTime.now();
            try (PreparedStatement ps = connection.prepareStatement(sql); ResultSet resultSet = ps.executeQuery()) {
                resultSet.setFetchSize(fetchSize);
                AtomicInteger rowCount = new AtomicInteger(0);
                LocalDateTime endQuery = LocalDateTime.now();
                if (kafkaOperate.isMultiplePartitions()) {
                    while (resultSet.next()) {
                        context.resultSetMultiplePartitionsHandler(resultSet);
                        rowCount.incrementAndGet();
                    }
                } else {
                    while (resultSet.next()) {
                        context.resultSetSinglePartitionHandler(resultSet);
                        rowCount.incrementAndGet();
                    }
                }
                context.resultFlush();
                LocalDateTime end = LocalDateTime.now();
                log.debug("{},fetch and send record {},query-cost={} send-result-cost={} all-cost={}",
                    task.getTableName(), rowCount.get(), Duration.between(start, endQuery).toMillis(),
                    Duration.between(endQuery, end).toMillis(), Duration.between(start, end).toMillis());
                return rowCount.get();
            }
        }

        /**
         * use a jdbc connection to query sql ,and parse and hash query result.then hash result send kafka topic
         *
         * @param connection connection
         * @param sliceSql   sliceSql
         * @param context    context
         * @param fetchSize  fetchSize
         * @return resultSize
         * @throws SQLException SQLException
         */
        public int resultSetHandlerParallelContext(Connection connection, String sliceSql, QueryTableRowContext context,
            int fetchSize) throws SQLException {
            QueryTableRowContext parallelContext =
                new QueryTableRowContext(context.getTableMetadata(), databaseType, kafkaOperate);
            return resultSetHandler(connection, sliceSql, parallelContext, fetchSize);
        }

        /**
         * start openGauss query dop
         *
         * @param queryDop queryDop
         * @throws SQLException SQLException
         */
        public void enableDatabaseParallelQuery(int queryDop) throws SQLException {
            if (Objects.equals(DataBaseType.OG, databaseType)) {
                Connection connection = getConnectionAndClosedAutoCommit();
                try(PreparedStatement ps = connection.prepareStatement(String.format(OPEN_GAUSS_PARALLEL_QUERY, queryDop))) {
                    ps.execute();
                }
                DataSourceUtils.doReleaseConnection(connection, jdbcDataSource);
            }
        }

        /**
         * get parallel query dop
         *
         * @return query dop
         */
        public int getParallelQueryDop() {
            return resourceManager.getParallelQueryDop();
        }

        private void takeConnection(long free) {
            while (!resourceManager.canExecQuery(free)) {
                if (isShutdown()) {
                    break;
                }
                ThreadUtil.sleep(50);
            }
        }

        private void takeConnection() {
            while (!resourceManager.canExecQuery()) {
                if (isShutdown()) {
                    break;
                }
                ThreadUtil.sleep(50);
            }
        }

        private boolean isShutdown() {
            return resourceManager.isShutdown();
        }
    }

    /**
     * kafka operations
     */
    class KafkaOperations {
        private static final int DEFAULT_PARTITION = 0;
        private static final int MIN_PARTITION_NUM = 1;
        private static final int MAX_TRY_CREATE_TOPIC_TIME = 600;

        private final KafkaTemplate<String, String> kafkaTemplate;
        private final KafkaAdminService kafkaAdminService;
        private int topicPartitionCount;
        private String topicName;
        private String endpointTopicPrefix;

        /**
         * constructor
         *
         * @param kafkaTemplate     kafkaTemplate
         * @param kafkaAdminService kafkaAdminService
         */
        public KafkaOperations(KafkaTemplate<String, String> kafkaTemplate, KafkaAdminService kafkaAdminService) {
            this.kafkaTemplate = kafkaTemplate;
            this.kafkaAdminService = kafkaAdminService;
        }

        /**
         * init endpointTopicPrefix
         *
         * @param process  process
         * @param endpoint endpoint
         */
        public void init(String process, Endpoint endpoint) {
            this.endpointTopicPrefix = "CHECK_" + process + "_" + endpoint.getCode() + "_";
        }

        /**
         * create topic for table
         *
         * @param topic topic
         */
        public void createTopic(Topic topic) {
            TopicCache.add(topic);
            int tryCreateTopicTime = 0;
            // add kafka current limiting operation
            while (!kafkaAdminService.canCreateTopic(endpointTopicPrefix)) {
                log.debug("kafka's topic number is reached maximum-topic-size limits, {} wait ...",
                    topic.getTopicName());
                if (tryCreateTopicTime > MAX_TRY_CREATE_TOPIC_TIME) {
                    throw new CreateTopicTimeOutException(topicName);
                }
                tryCreateTopicTime++;
                ThreadUtil.sleepOneSecond();
            }
            kafkaAdminService.createTopic(topic.getTopicName(), topic.getPartitions());
            this.topicName = topic.getTopicName();
            this.topicPartitionCount = topic.getPartitions();
            if (!kafkaAdminService.isTopicExists(topicName)) {
                ThreadUtil.sleepOneSecond();
                if (!kafkaAdminService.isTopicExists(topicName)) {
                    kafkaAdminService.createTopic(topic.getTopicName(), topic.getPartitions());
                }
            }
            if (!kafkaAdminService.isTopicExists(topicName)) {
                throw new ExtractException("create topic has error : " + topic.toString());
            }
        }

        /**
         * send row data to topic,that has single partition
         *
         * @param row row
         */
        public void sendSinglePartitionRowData(RowDataHash row) {
            row.setPartition(DEFAULT_PARTITION);
            if (row.getPrimaryKeyHash() == 0) {
                log.debug("row data hash zero :{}:{}", row.getPrimaryKey(), row.toEncode());
                return;
            }
            kafkaTemplate.send(new ProducerRecord<>(topicName, DEFAULT_PARTITION, row.getPrimaryKey(), row.toEncode()));
        }

        /**
         * Flush the kafka producer.
         */
        public void flush() {
            kafkaTemplate.flush();
        }

        /**
         * send row data to topic,that has multiple partition
         *
         * @param row row
         */
        public void sendMultiplePartitionsRowData(RowDataHash row) {
            row.setPartition(calcSimplePartition(row.getPrimaryKeyHash()));
            kafkaTemplate
                .send(new ProducerRecord<>(topicName, row.getPartition(), row.getPrimaryKey(), row.toEncode()));
        }

        /**
         * this topic has multiple partitions
         *
         * @return true | false
         */
        public boolean isMultiplePartitions() {
            return topicPartitionCount > MIN_PARTITION_NUM;
        }

        private int calcSimplePartition(long value) {
            return (int) Math.abs(value % topicPartitionCount);
        }
    }

    /**
     * query table row context
     */
    static class QueryTableRowContext {
        private final ResultSetHashHandler resultSetHashHandler = new ResultSetHashHandler();
        private final TableMetadata tableMetadata;
        private final ResultSetHandler resultSetHandler;
        private final KafkaOperations kafkaOperate;
        private final List<String> columns;
        private final List<String> primary;

        /**
         * constructor
         *
         * @param tableMetadata tableMetadata
         * @param databaseType  databaseType
         * @param kafkaOperate  kafkaOperate
         */
        QueryTableRowContext(@NonNull TableMetadata tableMetadata, DataBaseType databaseType,
            KafkaOperations kafkaOperate) {
            this.resultSetHandler = new ResultSetHandlerFactory().createHandler(databaseType);
            this.tableMetadata = tableMetadata;
            this.kafkaOperate = kafkaOperate;
            this.columns = MetaDataUtil.getTableColumns(tableMetadata);
            this.primary = MetaDataUtil.getTablePrimaryColumns(tableMetadata);
        }

        /**
         * parse result set to RowDataHash
         *
         * @param rs rs
         * @return row data
         */
        public void resultSetMultiplePartitionsHandler(ResultSet rs) {
            kafkaOperate.sendMultiplePartitionsRowData(resultSetHandler(rs));
        }

        /**
         * parse result set to RowDataHash
         *
         * @param resultSet rs
         * @return row data
         */
        public void resultSetSinglePartitionHandler(ResultSet resultSet) {
            kafkaOperate.sendSinglePartitionRowData(resultSetHandler(resultSet));
        }

        /**
         * parse result set to RowDataHash
         *
         * @param rs rs
         * @return row data
         */
        private RowDataHash resultSetHandler(ResultSet rs) {
            return resultSetHashHandler.handler(primary, columns, resultSetHandler.putOneResultSetToMap(rs));
        }

        /**
         * getTableMetadata
         *
         * @return TableMetadata
         */
        public TableMetadata getTableMetadata() {
            return tableMetadata;
        }

        /**
         * getTableName
         *
         * @return TableName
         */
        public String getTableName() {
            return tableMetadata.getTableName();
        }

        /**
         * getAvgRowLength
         *
         * @return tAvgRowLength
         */
        public long getAvgRowLength() {
            return tableMetadata.getAvgRowLength();
        }

        /**
         * getTableRows
         *
         * @return TableRows
         */
        public long getTableRows() {
            return tableMetadata.getTableRows();
        }

        /**
         * Flush the kafka producer.
         */
        public synchronized void resultFlush() {
            kafkaOperate.flush();
        }
    }
}
