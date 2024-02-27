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

package org.opengauss.datachecker.extract.data.csv;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.csv.SliceIndexVo;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.enums.SliceIndexStatus;
import org.opengauss.datachecker.common.entry.enums.SliceLogType;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.MapUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.constants.ExtConstants;

import java.io.File;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * CsvWriterListener
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/17
 * @since ：11
 */
public class CsvWriterListener implements CsvListener {
    private static final Logger log = LogUtils.getLogger();
    private static final String SCHEMA = "schema";
    private static final String TABLE_INDEX_COMPLETED_FEEDBACK = "table-index-completed-feedback";

    private final Map<String, List<SliceVo>> writerSliceMap = new ConcurrentSkipListMap<>();
    private volatile BlockingQueue<String> tableIndexCompletedList = new LinkedBlockingQueue<>();
    private volatile Set<Long> logDuplicateCheck = new HashSet<>();

    private Tailer tailer;
    private boolean isTailEnd = false;
    private CheckingFeignClient checkingClient;
    private ExecutorService feedbackExecutor;
    private boolean isFeedbackRunning = true;

    @Override
    public void initCsvListener(CheckingFeignClient checkingClient) {
        this.checkingClient = checkingClient;
        log.info("csv writer listener is starting .");
        // creates and starts a tailer for read writer logs in real time
        tailer = Tailer.create(new File(ConfigCache.getWriter()), new TailerListenerAdapter() {
            @Override
            public void handle(String line) {
                try {
                    isTailEnd = StringUtils.equalsIgnoreCase(line, ExtConstants.CSV_LISTENER_END);
                    if (isTailEnd) {
                        log.info("the writer is end, stopped the tailer listener : {}", line);
                        tailer.stop();
                        return;
                    }
                    long contentHash = lineContentHash(line);
                    if (logDuplicateCheck.contains(contentHash)) {
                        log.warn("writer log duplicate : {}", line);
                        return;
                    }
                    logDuplicateCheck.add(contentHash);

                    JSONObject writeLog = JSONObject.parseObject(line);
                    if (skipNoInvalidSlice(writeLog)) {
                        log.warn("writer skip no invalid slice log : {}", line);
                        return;
                    }
                    String schema = writeLog.getString(SCHEMA);
                    if (skipNoMatchSchema(ConfigCache.getSchema(), schema)) {
                        log.warn("writer skip no match schema log : {}", line);
                        return;
                    }
                    SliceLogType sliceLogType = SliceLogType.valueOf(writeLog.getString("type"));
                    if (Objects.equals(sliceLogType, SliceLogType.SLICE)) {
                        SliceVo slice = JSONObject.parseObject(line, SliceVo.class);
                        checkSlicePtnNum(slice);
                        MapUtils.put(writerSliceMap, slice.getTable(), slice);
                    } else if (Objects.equals(sliceLogType, SliceLogType.INDEX)) {
                        SliceIndexVo sliceIndex =
                            JSONObject.parseObject(line, SliceIndexVo.class, Feature.AllowISO8601DateFormat);
                        if (Objects.equals(sliceIndex.getIndexStatus(), SliceIndexStatus.END) || Objects.equals(
                            sliceIndex.getIndexStatus(), SliceIndexStatus.NONE)) {
                            tableIndexCompletedList.add(sliceIndex.getTable());
                        }
                    }
                    log.debug("writer add log : {}", line);
                } catch (Exception ex) {
                    log.error("writer log listener error : {}", line, ex);
                }
            }
        }, ConfigCache.getCsvLogMonitorInterval(), false);
        startNotifyExecutor();
        log.info("csv writer listener is started.");
    }

    private void startNotifyExecutor() {
        feedbackExecutor = ThreadUtil.newSingleThreadExecutor();
        int interval = ConfigCache.getIntValue(ConfigConstants.CSV_TASK_DISPATCHER_INTERVAL) * 1000;
        int maxDispatcherSize = ConfigCache.getIntValue(ConfigConstants.CSV_MAX_DISPATCHER_SIZE);
        feedbackExecutor.submit(() -> {
            Thread.currentThread().setName(TABLE_INDEX_COMPLETED_FEEDBACK);
            List<String> completedTableList = new LinkedList<>();
            while (isFeedbackRunning) {
                try {
                    if (tableIndexCompletedList.size() > 0) {
                        while (!tableIndexCompletedList.isEmpty() && completedTableList.size() < maxDispatcherSize) {
                            completedTableList.add(tableIndexCompletedList.poll());
                        }
                        if (CollectionUtils.isNotEmpty(completedTableList)) {
                            checkingClient.notifyTableIndexCompleted(completedTableList);
                            log.info("notify table can start checking [{}]", completedTableList);
                        }
                    }
                } catch (Exception ignore) {
                    log.warn(" retry notifyTableIndexCompleted {}", completedTableList);
                    tableIndexCompletedList.addAll(completedTableList);
                } finally {
                    completedTableList.clear();
                }
                ThreadUtil.sleep(interval);
            }
        });
    }

    @Override
    public List<SliceVo> fetchTableSliceList(String table) {
        return writerSliceMap.get(table);
    }

    @Override
    public void releaseSliceCache(String table) {
        writerSliceMap.remove(table);
    }

    @Override
    public void stop() {
        if (Objects.nonNull(tailer)) {
            tailer.stop();
        }
        writerSliceMap.clear();
        logDuplicateCheck.clear();
        isFeedbackRunning = false;
        if (Objects.nonNull(feedbackExecutor)) {
            feedbackExecutor.shutdown();
        }
    }

    @Override
    public void notifyCheckIgnoreTable(String tableName, String reason) {
        checkingClient.notifyCheckIgnoreTable(Endpoint.SINK, tableName, reason);
    }

    @Override
    public boolean isFinished() {
        return isTailEnd && writerSliceMap.isEmpty();
    }

    private boolean skipNoInvalidSlice(JSONObject slice) {
        return Objects.isNull(slice) || slice.size() == 0;
    }

    private boolean skipNoMatchSchema(String schema, String logSchema) {
        return !Objects.equals(logSchema, schema);
    }
}
