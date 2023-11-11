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
import org.opengauss.datachecker.extract.slice.common.SliceKafkaAgents;
import org.opengauss.datachecker.extract.slice.common.SliceResultSetSender;
import org.opengauss.datachecker.extract.task.sql.FullQueryStatement;
import org.opengauss.datachecker.extract.task.sql.QuerySqlEntry;
import org.opengauss.datachecker.extract.task.sql.SliceQueryStatement;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import javax.sql.DataSource;
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

/**
 * JdbcSliceProcessor
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/8
 * @since ：11
 */
public class JdbcSliceProcessor extends AbstractSliceProcessor {

    private final JdbcDataOperations jdbcOperation;

    public JdbcSliceProcessor(DataSource datasource, SliceVo slice, SliceProcessorContext context) {
        super(slice, context);
        this.jdbcOperation = context.getJdbcDataOperations(datasource);
    }

    @Override
    public void run() {
        log.info("table slice [{}] is beginning to extract data", slice.toSimpleString());
        try {
            TableMetadata tableMetadata = context.getTableMetaData(slice.getTable());
            SliceExtend sliceExtend = createSliceExtend(tableMetadata.getTableHash());
            if (!slice.isEmptyTable()) {
                QuerySqlEntry queryStatement = createQueryStatement(tableMetadata);
                executeQueryStatement(queryStatement, tableMetadata, sliceExtend);
            } else {
                log.info("table slice [{}] is empty ", slice.toSimpleString());
            }
            feedbackStatus(sliceExtend);
        } catch (Exception ex) {
            log.error("table slice [{}] is error", slice.toSimpleString(), ex);
        } finally {
            log.info("table slice [{}] is finally ", slice.toSimpleString());
        }
    }

    protected QuerySqlEntry createQueryStatement(TableMetadata tableMetadata) {
        if (slice.isSlice()) {
            SliceQueryStatement sliceStatement = context.createSliceQueryStatement();
            return sliceStatement.buildSlice(tableMetadata, slice);
        } else {
            FullQueryStatement queryStatement = context.createFullQueryStatement();
            return queryStatement.builderByTaskOffset(tableMetadata);
        }
    }

    protected SliceExtend executeQueryStatement(QuerySqlEntry sqlEntry, TableMetadata tableMetadata,
        SliceExtend sliceExtend) {
        final LocalDateTime start = LocalDateTime.now();
        int rowCount = 0;
        Connection connection = null;
        long jdbcQueryCost = 0;
        long sendDataCost = 0;
        long sliceAllCost = 0;

        try {
            long estimatedRowCount = slice.isSlice() ? slice.getFetchSize() : tableMetadata.getTableRows();
            long estimatedMemorySize = estimatedMemorySize(tableMetadata.getAvgRowLength(), estimatedRowCount);
            connection = jdbcOperation.tryConnectionAndClosedAutoCommit(estimatedMemorySize);
            SliceKafkaAgents kafkaAgents = context.createSliceKafkaAgents(slice);
            SliceResultSetSender sliceSender = new SliceResultSetSender(tableMetadata, kafkaAgents);

            try (PreparedStatement ps = connection.prepareStatement(sqlEntry.getSql());
                ResultSet resultSet = ps.executeQuery()) {
                LocalDateTime jdbcQuery = LocalDateTime.now();
                jdbcQueryCost = durationBetweenToMillis(start, jdbcQuery);
                resultSet.setFetchSize(FETCH_SIZE);
                ResultSetMetaData rsmd = resultSet.getMetaData();
                sliceExtend.setStartOffset(sliceSender.checkOffsetEnd());
                Map<String, String> result = new TreeMap<>();
                List<long[]> offsetList = new LinkedList<>();
                List<ListenableFuture<SendResult<String, String>>> batchFutures = new LinkedList<>();
                while (resultSet.next()) {
                    rowCount++;
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
                sliceExtend.setStartOffset(getMinOffset(offsetList));
                sliceExtend.setEndOffset(getMaxOffset(offsetList));
                sliceExtend.setCount(rowCount);
                sendDataCost = durationBetweenToMillis(jdbcQuery, LocalDateTime.now());
            }
            return sliceExtend;
        } catch (Exception ex) {
            log.error("jdbc query [{}] error : {}", sliceExtend.toSimpleString(), ex.getMessage());
            throw new ExtractDataAccessException();
        } finally {
            jdbcOperation.releaseConnection(connection);
            sliceAllCost = durationBetweenToMillis(start, LocalDateTime.now());
            log.info("query slice [{}] cost [{} /{} /{}] milliseconds", sliceExtend.toSimpleString(), jdbcQueryCost,
                sendDataCost, sliceAllCost);
        }
    }
}
