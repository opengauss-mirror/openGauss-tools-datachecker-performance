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

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.fastjson.JSON;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.constant.DynamicTpConstant;
import org.opengauss.datachecker.common.entry.common.ExtractContext;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;
import org.opengauss.datachecker.common.exception.ExtractException;
import org.opengauss.datachecker.common.service.DynamicThreadPoolManager;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.TaskUtilHelper;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.resource.ResourceManager;
import org.opengauss.datachecker.extract.task.sql.FullQueryStatement;
import org.opengauss.datachecker.extract.task.sql.QuerySqlEntry;
import org.opengauss.datachecker.extract.task.sql.QueryStatementFactory;
import org.opengauss.datachecker.extract.task.sql.AutoSliceQueryStatement;
import org.opengauss.datachecker.extract.util.HashHandler;
import org.opengauss.datachecker.extract.util.MetaDataUtil;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.lang.NonNull;

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
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.opengauss.datachecker.common.constant.DynamicTpConstant.TABLE_PARALLEL_EXECUTOR;

/**
 * Data extraction thread class
 *
 * @author wang chao
 * @date 2022/5/12 19:17
 * @since 11
 **/
public class ExtractTaskRunnable implements Runnable {
    private static final Logger log = LogUtils.getBusinessLogger();
    private static final Logger logKafka = LogUtils.getKafkaLogger();
    private static final int FETCH_SIZE = 10000;
    private final ExtractTask task;
    private final DynamicThreadPoolManager dynamicThreadPoolManager;
    private final CheckingFeignClient checkingFeignClient;
    private final KafkaOperations kafkaOperate;
    private final ExtractContext extractContext;
    private final CheckPoint checkPoint;
    private TaskUtilHelper taskUtilHelper;
    private final QueryStatementFactory factory = new QueryStatementFactory();
    private final AtomicReference<JdbcDataExtractionOperations> jdbcOperation;

    /**
     * Thread Constructor
     *
     * @param processNo processNo
     * @param task      task information
     * @param support   Thread helper class
     */
    public ExtractTaskRunnable(String processNo, ExtractTask task, ExtractThreadSupport support) {
        this.task = task;
        JdbcDataExtractionOperations jdbcOperate =
            new JdbcDataExtractionOperations(support.getDataSource(), support.getResourceManager());
        this.jdbcOperation = new AtomicReference<>(jdbcOperate);
        this.checkingFeignClient = support.getCheckingFeignClient();
        this.dynamicThreadPoolManager = support.getDynamicThreadPoolManager();
        this.extractContext = support.getContext();
        this.kafkaOperate = new KafkaOperations(support.getKafkaTemplate());
        this.checkPoint = new CheckPoint(support.getDataAccessService());

    }

    @Override
    public void run() {
        try {
            log.info("table [{}] extract task has beginning", task.getTableName());
            kafkaOperate.setTopic(task.getTopic());
            QueryTableRowContext context = new QueryTableRowContext(task.getTableMetadata(), kafkaOperate);
            taskUtilHelper = new TaskUtilHelper(context.getTableMetadata(), extractContext.getMaximumTableSliceSize());
            seekExtractTableInfo(context.getTableMetadata());
            if (isNotSlice()) {
                executeTask(context);
            } else {
                executeMultiTaskOffset(context);
            }
            checkingFeignClient.refreshTableExtractStatus(task.getTableName(), extractContext.getEndpoint(),
                extractContext.getEndpoint()
                              .getCode());
            log.info("refresh table {} extract status success", task.getTableName());
        } catch (Exception ex) {
            checkingFeignClient.refreshTableExtractStatus(task.getTableName(), extractContext.getEndpoint(), -1);
            log.error("extract", ex);
        } finally {
            Runtime.getRuntime()
                   .gc();
        }
    }

    private void executeTask(QueryTableRowContext context) {
        final LocalDateTime start = LocalDateTime.now();
        Connection connection = null;
        int rowCount = 0;
        try {
            FullQueryStatement fullQueryStatement = factory.createFullQueryStatement();
            QuerySqlEntry querySqlEntry = fullQueryStatement.builderByTaskOffset(context.getTableMetadata());
            connection = jdbcOperation.get().tryConnectionAndClosedAutoCommit(context.evaluateMemorySize());
            rowCount = jdbcOperation.get().resultSetHandler(connection, querySqlEntry, context, FETCH_SIZE);
        } catch (SQLException ex) {
            log.error("jdbc query  {} error : {}", context.getTableName(), ex.getMessage());
            throw new ExtractDataAccessException();
        } finally {
            jdbcOperation.get().releaseConnection(connection);
            log.info("query table [{}] row-count [{}] cost [{}] milliseconds", context.getTableName(), rowCount,
                Duration.between(start, LocalDateTime.now())
                        .toMillis());
            logNumberOfGlobalTasks(context.getTableName(), null,
                dynamicThreadPoolManager.getExecutor(DynamicTpConstant.EXTRACT_EXECUTOR));
        }
    }

    private void executeMultiTaskOffset(QueryTableRowContext context) {
        LocalDateTime start = LocalDateTime.now();
        int slice = extractContext.getMaximumTableSliceSize();
        long totalRows = 0;
        try {
            AutoSliceQueryStatement sliceStatement =
                factory.createSliceQueryStatement(checkPoint, context.getTableMetadata());
            List<QuerySqlEntry> querySqlList = sliceStatement.builderByTaskOffset(context.getTableMetadata(), slice);
            if (CollectionUtils.isNotEmpty(querySqlList)) {
                totalRows = executeParallelTask(querySqlList, context);
            }
            querySqlList.clear();
        } catch (Exception ex) {
            log.error("jdbc parallel query has unknown error [{}] : ", context.getTableName(), ex);
            throw new ExtractDataAccessException();
        } finally {
            log.info("table [{}] execution [{}] rows completed, taking a total of {} milliseconds",
                context.getTableName(), totalRows, Duration.between(start, LocalDateTime.now())
                                                           .toMillis());
        }
    }

    private long executeParallelTask(List<QuerySqlEntry> querySqlList, QueryTableRowContext context)
        throws SQLException, InterruptedException {
        ThreadPoolExecutor executor = dynamicThreadPoolManager.getExecutor(TABLE_PARALLEL_EXECUTOR);
        AtomicLong totalRowCount = new AtomicLong(0);
        AtomicInteger exceptionCount = new AtomicInteger(0);
        int sliceSize = extractContext.getMaximumTableSliceSize();
        CountDownLatch countDownLatch = new CountDownLatch(querySqlList.size());
        enableParallelQueryDop(querySqlList.size());
        querySqlList.forEach(queryEntry -> {
            executor.submit(() -> {
                Connection connection = null;
                try {
                    connection = jdbcOperation.get().tryConnectionAndClosedAutoCommit(context.evaluateMemorySize(sliceSize));
                    totalRowCount.addAndGet(
                        jdbcOperation.get().resultSetHandlerParallelContext(connection, queryEntry, context, FETCH_SIZE));
                } catch (SQLException ex) {
                    exceptionCount.incrementAndGet();
                    log.error("jdbc parallel query [{}] error : {}", queryEntry.getSql(), ex.getMessage());
                    throw new ExtractDataAccessException();
                } finally {
                    countDown(context.getTableName(), countDownLatch, executor);
                    jdbcOperation.get().releaseConnection(connection);
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
        return totalRowCount.get();
    }

    private void seekExtractTableInfo(TableMetadata tableMetadata) {
        log.info("table [{}] isAutoIncrement=[{}] , column=[{}] , avgRowLength=[{}] , tableRows=[{}] ",
            tableMetadata.getTableName(), tableMetadata.isAutoIncrement(), tableMetadata.getColumnsMetas()
                                                                                        .size(),
            tableMetadata.getAvgRowLength(), tableMetadata.getTableRows());
        log.info("table [{}] table column metadata -> {}", tableMetadata.getTableName(),
            getTableColumnInformation(tableMetadata));
    }

    private void enableParallelQueryDop(int taskOffset) throws SQLException {
        int dop = Math.min(taskOffset, jdbcOperation.get().getParallelQueryDop());
        jdbcOperation.get().enableDatabaseParallelQuery(dop);
    }

    private String getTableColumnInformation(TableMetadata tableMetadata) {
        return tableMetadata.getColumnsMetas()
                            .stream()
                            .map(ColumnsMetaData::getColumnMsg)
                            .collect(Collectors.joining(" , "));
    }

    private boolean isNotSlice() {
        return taskUtilHelper.noTableSlice() || jdbcOperation.get().getParallelQueryDop() == 1;
    }

    private void countDown(String tableName, CountDownLatch countDownLatch, ThreadPoolExecutor executor) {
        countDownLatch.countDown();
        logNumberOfGlobalTasks(tableName, countDownLatch, executor);
    }

    private void logNumberOfGlobalTasks(String tableName, CountDownLatch countDownLatch, ThreadPoolExecutor executor) {
        if (Objects.nonNull(countDownLatch) && countDownLatch.getCount() > 0) {
            log.info("table [{}] remaining [{}] tasks ,tpExecutor core={} , all:[completed={}, remain={},running={}]",
                tableName, countDownLatch.getCount(), executor.getCorePoolSize(), executor.getCompletedTaskCount(),
                executor.getTaskCount() - executor.getCompletedTaskCount(), executor.getActiveCount());
        } else {
            log.info("table [{}] tpExecutor core={}  all:[completed={}, remain={},running={}]", tableName,
                executor.getCorePoolSize(), executor.getCompletedTaskCount(),
                executor.getTaskCount() - executor.getCompletedTaskCount(), executor.getActiveCount());
        }
    }

    /**
     * jdbc data extraction operations
     */
    class JdbcDataExtractionOperations {
        private static final String OPEN_GAUSS_PARALLEL_QUERY = "set query_dop to %s;";
        private static final int LOG_WAIT_TIMES = 600;

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
        public synchronized Connection tryConnectionAndClosedAutoCommit(long allocMemory) throws SQLException {
            takeConnection(allocMemory);
            return getConnectionAndClosedAutoCommit();
        }

        /**
         * try to get a jdbc connection and close auto commit.
         *
         * @return Connection
         * @throws SQLException SQLException
         */
        public synchronized Connection tryConnectionAndClosedAutoCommit() throws SQLException {
            takeConnection(0);
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
         * @param sqlEntry   sqlEntry
         * @param context    context
         * @param fetchSize  fetchSize
         * @return resultSize
         * @throws SQLException SQLException
         */
        public int resultSetHandler(Connection connection, QuerySqlEntry sqlEntry, QueryTableRowContext context,
            int fetchSize) throws SQLException {
            LocalDateTime start = LocalDateTime.now();
            try (PreparedStatement ps = connection.prepareStatement(sqlEntry.getSql());
                ResultSet resultSet = ps.executeQuery()) {
                LocalDateTime endQuery = LocalDateTime.now();
                resultSet.setFetchSize(fetchSize);
                ResultSetMetaData rsmd = resultSet.getMetaData();
                int rowCount = 0;
                Map<String, String> result = new TreeMap<>();
                if (kafkaOperate.isMultiplePartitions()) {
                    while (resultSet.next()) {
                        context.resultSetMultiplePartitionsHandler(rsmd, resultSet, result);
                        rowCount++;
                        logProcessTableRowNum(context, sqlEntry, rowCount);
                    }
                } else {
                    while (resultSet.next()) {
                        context.resultSetSinglePartitionHandler(rsmd, resultSet, result);
                        rowCount++;
                        logProcessTableRowNum(context, sqlEntry, rowCount);
                    }
                }
                context.resultFlush();
                logSliceQueryInfo(sqlEntry, start, rowCount, endQuery, LocalDateTime.now());
                return rowCount;
            }
        }

        private void logSliceQueryInfo(QuerySqlEntry sqlEntry, LocalDateTime start, int rowCount,
            LocalDateTime endQuery, LocalDateTime end) {
            log.info(" extract table {} , row-count=[{}] completed , cost=[ query={} send={} all={} ]",
                sqlEntry.toString(), rowCount, Duration.between(start, endQuery)
                                                       .toMillis(), Duration.between(endQuery, end)
                                                                            .toMillis(), Duration.between(start, end)
                                                                                                 .toMillis());
        }

        private void logProcessTableRowNum(QueryTableRowContext context, QuerySqlEntry sqlEntry, int rowCount) {
            if (rowCount % FETCH_SIZE == 0) {
                log.debug(" extract table {} , row-count=[{}]", sqlEntry.toString(), rowCount);
                context.resultFlush();
            }
        }

        /**
         * use a jdbc connection to query sql ,and parse and hash query result.then hash result send kafka topic
         *
         * @param connection connection
         * @param sqlEntry   sqlEntry
         * @param context    context
         * @param fetchSize  fetchSize
         * @return resultSize
         * @throws SQLException SQLException
         */
        public int resultSetHandlerParallelContext(Connection connection, QuerySqlEntry sqlEntry,
            QueryTableRowContext context, int fetchSize) throws SQLException {
            QueryTableRowContext parallelContext = new QueryTableRowContext(context.getTableMetadata(), kafkaOperate);
            return resultSetHandler(connection, sqlEntry, parallelContext, fetchSize);
        }

        /**
         * start openGauss query dop
         *
         * @param queryDop queryDop
         * @throws SQLException SQLException
         */
        public void enableDatabaseParallelQuery(int queryDop) throws SQLException {
            if (Objects.equals(DataBaseType.OG, extractContext.getDatabaseType())) {
                Connection connection = getConnectionAndClosedAutoCommit();
                try (PreparedStatement ps = connection.prepareStatement(
                    String.format(OPEN_GAUSS_PARALLEL_QUERY, queryDop))) {
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
            int waitTimes = 0;
            while (!canExecQuery(free)) {
                if (isShutdown()) {
                    break;
                }
                ThreadUtil.sleepOneSecond();
                waitTimes++;
                if (waitTimes >= LOG_WAIT_TIMES) {
                    log.warn("table {}  try to take connection", task.getTableName());
                    waitTimes = 0;
                }
            }
        }

        private boolean canExecQuery(long free) {
            return free > 0 ? resourceManager.canExecQuery(free) : resourceManager.canExecQuery();
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

        private final KafkaTemplate<String, String> kafkaTemplate;

        private int topicPartitionCount;
        private String topicName;

        /**
         * constructor
         *
         * @param kafkaTemplate kafkaTemplate
         */
        public KafkaOperations(KafkaTemplate<String, String> kafkaTemplate) {
            this.kafkaTemplate = kafkaTemplate;
        }

        /**
         * create topic for table
         *
         * @param topic topic
         */
        public void setTopic(Topic topic) {
            this.topicName = topic.getTopicName(extractContext.getEndpoint());
            this.topicPartitionCount = topic.getPartitions();
        }

        /**
         * send row data to topic,that has multiple partition
         *
         * @param row row
         */
        public void sendMultiplePartitionsRowData(RowDataHash row) {
            int ptn = calcSimplePartition(row.getKHash());
            send(topicName, ptn, row.getKey(), JSON.toJSONString(row));
        }

        /**
         * send row data to topic,that has single partition
         *
         * @param row row
         */
        public void sendSinglePartitionRowData(RowDataHash row) {
            if (row.getKHash() == 0) {
                log.debug("row data hash zero :{}:{}", row.getKey(), JSON.toJSONString(row));
                return;
            }
            send(topicName, DEFAULT_PARTITION, row.getKey(), JSON.toJSONString(row));
        }

        private void send(String topicName, int partition, String key, String message) {
            try {
                kafkaTemplate.send(new ProducerRecord<>(topicName, partition, key, message));
            } catch (Exception kafkaException) {
                logKafka.error("send kafka [{} , {}] record error {}", topicName, key, kafkaException);
                throw new ExtractException(
                    "send kafka [" + topicName + " , " + key + "] record " + kafkaException.getMessage());
            }
        }

        /**
         * Flush the kafka producer.
         */
        public void flush() {
            kafkaTemplate.flush();
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
        private static final HashHandler hashHandler = new HashHandler();
        private final TableMetadata tableMetadata;
        private final ResultSetHandler resultSetHandler;
        private final KafkaOperations kafkaOperate;
        private final List<String> columns;
        private final List<String> primary;

        /**
         * constructor
         *
         * @param tableMetadata tableMetadata
         * @param kafkaOperate  kafkaOperate
         */
        QueryTableRowContext(@NonNull TableMetadata tableMetadata, KafkaOperations kafkaOperate) {
            this.resultSetHandler = new ResultSetHandlerFactory().createHandler(tableMetadata.getDataBaseType());
            this.tableMetadata = tableMetadata;
            this.kafkaOperate = kafkaOperate;
            this.columns = MetaDataUtil.getTableColumns(tableMetadata);
            this.primary = MetaDataUtil.getTablePrimaryColumns(tableMetadata);
        }

        /**
         * parse result set to RowDataHash
         *
         * @param rs rs
         */
        public void resultSetMultiplePartitionsHandler(ResultSetMetaData rsmd, ResultSet rs,
            Map<String, String> result) {
            resultSetHandler.putOneResultSetToMap(rsmd, rs, result);
            RowDataHash dataHash = handler(primary, columns, result);
            result.clear();
            kafkaOperate.sendMultiplePartitionsRowData(dataHash);
        }

        /**
         * parse result set to RowDataHash
         *
         * @param rs rs
         */
        public void resultSetSinglePartitionHandler(ResultSetMetaData rsmd, ResultSet rs, Map<String, String> result) {
            resultSetHandler.putOneResultSetToMap(rsmd, rs, result);
            RowDataHash dataHash = handler(primary, columns, result);
            result.clear();
            kafkaOperate.sendSinglePartitionRowData(dataHash);
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

        public long getMaxRowLength() {
            return tableMetadata.getAvgRowLength() * 2;
        }

        /**
         * Flush the kafka producer.
         */
        public synchronized void resultFlush() {
            kafkaOperate.flush();
        }

        public long evaluateMemorySize(long sliceSize) {
            return (getMaxRowLength() + 30) * sliceSize;
        }

        public long evaluateMemorySize() {
            return evaluateMemorySize(tableMetadata.getTableRows());
        }
    }
}
