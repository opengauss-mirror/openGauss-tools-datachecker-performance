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
import com.opencsv.exceptions.CsvValidationException;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.resource.MemoryOperations;
import org.opengauss.datachecker.extract.slice.SliceProcessorContext;
import org.opengauss.datachecker.extract.slice.common.SliceKafkaAgents;
import org.opengauss.datachecker.extract.slice.common.SliceResultSetSender;
import org.springframework.kafka.support.SendResult;
import java.util.concurrent.CompletableFuture;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * CsvSliceProcessor
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/8
 * @since ：11
 */
public class CsvSliceProcessor extends AbstractSliceProcessor {
    private static final Logger log = LogUtils.getLogger(CsvSliceProcessor.class);
    protected MemoryOperations memoryOperations;

    public CsvSliceProcessor(SliceVo slice, SliceProcessorContext context) {
        super(slice, context);
        this.memoryOperations = context.getMemoryDataOperations();
    }

    @Override
    public void run() {
        SliceExtend sliceExtend = null;
        try {
            LogUtils.info(log,"csv slice processor start , [{}]", slice.toSimpleString());
            TableMetadata tableMetadata = context.getTableMetaData(slice.getTable());
            sliceExtend = createSliceExtend(tableMetadata.getTableHash());
            executeQueryStatement(tableMetadata, sliceExtend);
        } catch (Exception ex) {
            LogUtils.error(log,"csv slice processor , [{}] : ", slice.toSimpleString(), ex);
        } finally {
            feedbackStatus(sliceExtend);
            memoryOperations.release();
            LogUtils.info(log,"csv slice processor finally ,[{}]", slice.toSimpleString());
        }
    }

    private void executeQueryStatement(TableMetadata tableMetadata, SliceExtend sliceExtend) throws IOException {
        final LocalDateTime start = LocalDateTime.now();
        int rowCount = 0;
        try {
            String csvDataRootPath = ConfigCache.getCsvData();
            String sliceFilePath = Path.of(csvDataRootPath, slice.getName())
                                       .toString();
            long estimatedSize = estimatedMemorySize(tableMetadata.getAvgRowLength(), slice.getFetchSize());
            memoryOperations.takeMemory(estimatedSize);
            SliceKafkaAgents kafkaAgents = context.createSliceFixedKafkaAgents(topic, slice.getName());
            SliceResultSetSender sliceSender = new SliceResultSetSender(tableMetadata, kafkaAgents);
            sliceSender.setRecordSendKey(slice.getName());
            try (CSVReader reader = new CSVReader(new FileReader(sliceFilePath))) {
                LocalDateTime parseCsv = LocalDateTime.now();
                LogUtils.info(log,"parse slice [{}] cost [{}] milliseconds", sliceExtend.toSimpleString(),
                    durationBetweenToMillis(start, parseCsv));
                String[] nextLine;
                Map<String, String> result = new TreeMap<>();
                List<long[]> offsetList = new LinkedList<>();
                List<CompletableFuture<SendResult<String, String>>> batchFutures = new LinkedList<>();
                while ((nextLine = reader.readNext()) != null) {
                    rowCount++;
                    batchFutures.add(sliceSender.csvTranslateAndSendSync(nextLine, result, rowCount, slice.getNo()));
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
                LogUtils.info(log,"send slice [{}] cost [{}] milliseconds", sliceExtend.toSimpleString(),
                    durationBetweenToMillis(parseCsv, LocalDateTime.now()));
            }
            sliceExtend.setCount(rowCount);
        } catch (FileNotFoundException | CsvValidationException ex) {
            LogUtils.error(log,"csv parse [{}] error : {}", sliceExtend, ex.getMessage());
            throw new ExtractDataAccessException();
        } finally {
            LogUtils.info(log,"query slice [{}] cost [{}] milliseconds", sliceExtend.toSimpleString(),
                durationBetweenToMillis(start, LocalDateTime.now()));
        }
    }
}
