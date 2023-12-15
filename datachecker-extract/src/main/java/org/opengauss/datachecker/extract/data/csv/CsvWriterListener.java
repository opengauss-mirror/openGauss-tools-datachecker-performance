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
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.entry.csv.SliceIndexVo;
import org.opengauss.datachecker.common.entry.enums.SliceIndexStatus;
import org.opengauss.datachecker.common.entry.enums.SliceLogType;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.MapUtils;
import org.opengauss.datachecker.extract.constants.ExtConstants;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
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
    private final Map<String, List<SliceVo>> tableSliceLogs = new ConcurrentSkipListMap<>();
    private final BlockingQueue<SliceVo> listenerQueue = new LinkedBlockingQueue<>();
    private Tailer tailer;
    private TailerMonitor tailerMonitor;
    private boolean isTailEnd = false;

    @Override
    public void initCsvListener() {
        log.info("csv writer listener is starting .");
        // creates and starts a Tailer for read writer logs in real time
        tailer = Tailer.create(new File(ConfigCache.getWriter()), new TailerListenerAdapter() {
            @Override
            public void handle(String line) {
                try {
                    isTailEnd = StringUtils.equalsIgnoreCase(line, ExtConstants.CSV_LISTENER_END);
                    if (isTailEnd) {
                        log.info("writer tail end log ：{}", line);
                        stop();
                        return;
                    }
                    JSONObject writeLog = JSONObject.parseObject(line);
                    if (skipNoInvalidSlice(writeLog)) {
                        log.warn("writer skip no invalid slice log ：{}", line);
                        return;
                    }
                    String schema = writeLog.getString("schema");
                    if (skipNoMatchSchema(ConfigCache.getSchema(), schema)) {
                        log.warn("writer skip no match schema log ：{}", line);
                        return;
                    }
                    SliceLogType sliceLogType = SliceLogType.valueOf(writeLog.getString("type"));
                    if (Objects.equals(sliceLogType, SliceLogType.SLICE)) {
                        SliceVo slice = JSONObject.parseObject(line, SliceVo.class);
                        checkSlicePtnNum(slice);
                        MapUtils.put(tableSliceLogs, slice.getTable(), slice);
                    } else if (Objects.equals(sliceLogType, SliceLogType.INDEX)) {
                        SliceIndexVo sliceIndex =
                            JSONObject.parseObject(line, SliceIndexVo.class, Feature.AllowISO8601DateFormat);
                        if (Objects.equals(sliceIndex.getIndexStatus(), SliceIndexStatus.END)) {
                            List<SliceVo> sliceList = tableSliceLogs.get(sliceIndex.getTable());
                            listenerQueue.addAll(sliceList);
                            tableSliceLogs.remove(sliceIndex.getTable());
                            log.info("writer add table ：{}", sliceIndex.getTable());
                        } else if (Objects.equals(sliceIndex.getIndexStatus(), SliceIndexStatus.NONE)) {
                            tableSliceLogs.remove(sliceIndex.getTable());
                        }
                    }
                    log.debug("writer add log ：{}", line);
                } catch (Exception ex) {
                    log.error("writer log listener error ：{} : {}", ex.getMessage(), line);
                }
            }

        }, ConfigCache.getCsvLogMonitorInterval(), false);

        tailerMonitor = new TailerMonitor(tailer, listenerQueue);
        Thread thread = new Thread(tailerMonitor);
        thread.start();
        log.info("csv writer listener is started.");
    }

    @Override
    public SliceVo poll() {
        return listenerQueue.poll();
    }

    @Override
    public void stop() {
        if (Objects.nonNull(tailer)) {
            tailerMonitor.stop();
            tailer.stop();
            tableSliceLogs.clear();
        }
    }

    @Override
    public boolean isFinished() {
        return isTailEnd && listenerQueue.isEmpty();
    }

    private boolean skipNoInvalidSlice(JSONObject slice) {
        return Objects.isNull(slice) || slice.size() == 0;
    }

    private boolean skipNoMatchSchema(String schema, String logSchema) {
        return !Objects.equals(logSchema, schema);
    }
}
