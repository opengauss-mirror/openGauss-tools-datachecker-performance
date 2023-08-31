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
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.constants.ExtConstants;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * CsvReaderListener
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/17
 * @since ：11
 */
public class CsvReaderListener implements CsvListener {
    private static final Logger log = LogUtils.getLogger();
    private final BlockingQueue<SliceVo> listenerQueue = new LinkedBlockingQueue<>();
    private Tailer tailer;
    private TailerMonitor tailerMonitor;
    private boolean isTailEnd = false;

    @Override
    public void initCsvListener() {
        log.info("csv reader listener is starting .");
        // creates and starts a Tailer for read writer logs in real time
        tailer = Tailer.create(new File(ConfigCache.getReader()), new TailerListenerAdapter() {
            @Override
            public void handle(String line) {
                try {
                    isTailEnd = StringUtils.equalsIgnoreCase(line, ExtConstants.CSV_LISTENER_END);
                    SliceVo slice = JSONObject.parseObject(line, SliceVo.class);
                    if (skipNoInvalidSlice(slice)) {
                        return;
                    }
                    if (skipNoMatchSchema(ConfigCache.getSchema(), slice.getSchema())) {
                        return;
                    }
                    checkSlicePtnNum(slice);
                    listenerQueue.add(slice);
                    log.info("reader add log ：{}", line);
                } catch (Exception ex) {
                    log.error("reader log listener error ：" + ex.getMessage());
                }
            }
        }, ConfigCache.getCsvLogMonitorInterval(), false);

        tailerMonitor = new TailerMonitor(tailer, listenerQueue);
        Thread thread = new Thread(tailerMonitor);
        thread.start();
        log.info("csv reader listener is started .");
    }

    @Override
    public SliceVo poll() {
        return listenerQueue.poll();
    }

    @Override
    public boolean isFinished() {
        return isTailEnd && listenerQueue.isEmpty();
    }

    @Override
    public void stop() {
        if (Objects.nonNull(tailer)) {
            tailerMonitor.stop();
            tailer.stop();
        }
    }

    private boolean skipNoMatchSchema(String schema, String logSchema) {
        return !Objects.equals(logSchema, schema);
    }

    private boolean skipNoInvalidSlice(SliceVo slice) {
        return Objects.isNull(slice) || StringUtils.isEmpty(slice.getTable());
    }
}
