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

import com.opencsv.CSVReader;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.resource.MemoryOperations;
import org.opengauss.datachecker.extract.slice.SliceProcessorContext;
import org.opengauss.datachecker.extract.slice.common.SliceKafkaAgents;
import org.opengauss.datachecker.extract.slice.common.SliceResultSetSender;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;

/**
 * CsvTableProcessor
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/27
 * @since ：11
 */
public class CsvTableProcessor extends AbstractTableProcessor {
    private static final Logger log = LogUtils.getLogger(CsvTableProcessor.class);
    private static final int DEFAULT_CSV_FILE_ROW_FETCH_SIZE = 1000;

    protected MemoryOperations memoryOperations;
    protected List<Path> tableFilePaths;

    public CsvTableProcessor(String table, List<Path> tableFilePaths, SliceProcessorContext context) {
        super(table, context);
        this.tableFilePaths = tableFilePaths;
        this.memoryOperations = context.getMemoryDataOperations();
    }

    @Override
    public void run() {
        SliceExtend sliceExtend = createTableSliceExtend(tableFilePaths);
        try {
            long tableRowCount = 0;
            log.info("csv table processor start ,{} ,data file {}  : ", table,
                Objects.isNull(tableFilePaths) ? 0 : tableFilePaths.size());
            TableMetadata tableMetadata = context.getTableMetaData(table);
            if (CollectionUtils.isNotEmpty(tableFilePaths)) {
                tableRowCount = executeQueryStatement(tableMetadata, tableFilePaths, sliceExtend);
            } else {
                log.info("table [{}] is empty ", table);
            }
            sliceExtend.setCount(tableRowCount);
            feedbackStatus(sliceExtend);
        } catch (Exception ex) {
            log.error("{}csv table processor ,{} : ", ErrorCode.CSV_TABLE_PROCESSOR, table, ex);
            sliceExtend.setStatus(-1);
            feedbackStatus(sliceExtend);
        } finally {
            memoryOperations.release();
            log.info("csv table processor finally ,{} : ", table);
        }
    }

    private SliceExtend createTableSliceExtend(List<Path> tableFilePaths) {
        SliceExtend tableSliceExtend = createTableSliceExtend();
        tableSliceExtend.setTableFilePaths(tableFilePaths);
        return tableSliceExtend;
    }

    private long executeQueryStatement(TableMetadata tableMetadata, List<Path> tablePaths, SliceExtend tableSliceExtend)
        throws IOException {
        final LocalDateTime start = LocalDateTime.now();
        long tableRowCount = 0;
        SliceKafkaAgents kafkaAgents = context.createSliceFixedKafkaAgents(topic, table);
        SliceResultSetSender sliceSender = new SliceResultSetSender(tableMetadata, kafkaAgents);
        sliceSender.setRecordSendKey(table);
        try {
            String csvDataRootPath = ConfigCache.getCsvData();
            int tableFileCount = tablePaths.size();
            long fetchSize = DEFAULT_CSV_FILE_ROW_FETCH_SIZE;
            List<Long> minOffsetList = new LinkedList<>();
            List<Long> maxOffsetList = new LinkedList<>();
            for (int i = 1; i <= tableFileCount; i++) {
                Path slicePath = tablePaths.get(i - 1);
                log.info("start  [{}-{}] - {} ", tableFileCount, i, slicePath);
                Path sliceFilePath = Path.of(csvDataRootPath, slicePath.toString());
                fetchSize = Math.max(fetchSize, tableRowCount / i);
                long estimatedSize = estimatedMemorySize(tableMetadata.getAvgRowLength(), fetchSize);
                memoryOperations.takeMemory(estimatedSize);
                List<long[]> offsetList = new LinkedList<>();
                List<ListenableFuture<SendResult<String, String>>> batchFutures = new LinkedList<>();
                try (CSVReader reader = new CSVReader(new FileReader(sliceFilePath.toString()))) {
                    String[] nextLine;
                    int rowCount = 0;
                    Map<String, String> result = new TreeMap<>();
                    try {
                        while ((nextLine = reader.readNext()) != null) {
                            rowCount++;
                            batchFutures.add(sliceSender.csvTranslateAndSendSync(nextLine, result, rowCount, i));
                            if (batchFutures.size() == FETCH_SIZE) {
                                offsetList.add(getBatchFutureRecordOffsetScope(batchFutures));
                                batchFutures.clear();
                            }
                        }
                        if (batchFutures.size() > 0) {
                            offsetList.add(getBatchFutureRecordOffsetScope(batchFutures));
                            batchFutures.clear();
                        }
                        minOffsetList.add(getMinOffset(offsetList));
                        maxOffsetList.add(getMaxOffset(offsetList));
                    } catch (Exception ex) {
                        log.error("{}csvTranslateAndSend error: ", ErrorCode.CSV_TABLE_PROCESSOR, ex);
                    }
                    tableRowCount += rowCount;
                    log.info("finish [{}-{}] {} , [{} : {}]", tableFileCount, i, slicePath, rowCount, tableRowCount);
                } catch (Exception ex) {
                    log.error("{}CSVReader exception: ", ErrorCode.CSV_TABLE_PROCESSOR, ex);
                }
                memoryOperations.release();
            }
            tableSliceExtend.setStartOffset(getMinMinOffset(minOffsetList));
            tableSliceExtend.setEndOffset(getMaxMaxOffset(maxOffsetList));
            tableSliceExtend.setCount(tableRowCount);
        } catch (Exception ex) {
            log.error("{}jdbc query  {} error : {}", ErrorCode.CSV_TABLE_PROCESSOR, table, ex.getMessage());
            throw new ExtractDataAccessException();
        } finally {
            log.info("query slice [{}] cost [{}] milliseconds", table,
                Duration.between(start, LocalDateTime.now()).toMillis());
            kafkaAgents.agentsClosed();
        }
        return tableRowCount;
    }
}
