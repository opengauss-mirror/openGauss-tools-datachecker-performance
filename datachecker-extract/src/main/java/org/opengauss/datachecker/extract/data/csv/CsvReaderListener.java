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
import org.opengauss.datachecker.common.util.MapUtils;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.constants.ExtConstants;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CsvReaderListener
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/17
 * @since ：11
 */
public class CsvReaderListener implements CsvListener {
    private static final Logger log = LogUtils.getLogger();
    private final Map<String, List<SliceVo>> readerSliceMap = new ConcurrentHashMap<>();
    private volatile Set<Long> logDuplicateCheck = new HashSet<>();
    private Tailer tailer;
    private boolean isTailEnd = false;

    @Override
    public void initCsvListener(CheckingFeignClient checkingClient) {
        log.info("csv reader listener is starting .");
        // creates and starts a Tailer for read writer logs in real time
        tailer = Tailer.create(new File(ConfigCache.getReader()), new TailerListenerAdapter() {
            @Override
            public void handle(String line) {
                try {
                    isTailEnd = StringUtils.equalsIgnoreCase(line, ExtConstants.CSV_LISTENER_END);
                    if (isTailEnd) {
                        log.info("reader tail end log : {}", line);
                        stop();
                        return;
                    }
                    long contentHash = lineContentHash(line);
                    if (logDuplicateCheck.contains(contentHash)) {
                        log.warn("writer log duplicate : {}", line);
                        return;
                    }
                    logDuplicateCheck.add(contentHash);

                    SliceVo slice = JSONObject.parseObject(line, SliceVo.class);
                    if (skipNoInvalidSlice(slice)) {
                        log.warn("reader skip no invalid slice log : {}", line);
                        return;
                    }
                    if (skipNoMatchSchema(ConfigCache.getSchema(), slice.getSchema())) {
                        log.warn("reader skip no match schema log : {}", line);
                        return;
                    }
                    checkSlicePtnNum(slice);
                    MapUtils.put(readerSliceMap, slice.getTable(), slice);
                    log.debug("reader add log : {}", line);
                } catch (Exception ex) {
                    log.error("reader log listener error : " + ex.getMessage());
                }
            }
        }, ConfigCache.getCsvLogMonitorInterval(), false);
        log.info("csv reader listener is started .");
    }

    @Override
    public List<SliceVo> fetchTableSliceList(String table) {
        return readerSliceMap.get(table);
    }

    @Override
    public void releaseSliceCache(String table) {
        readerSliceMap.remove(table);
    }

    @Override
    public boolean isFinished() {
        return isTailEnd && readerSliceMap.isEmpty();
    }

    @Override
    public void stop() {
        if (Objects.nonNull(tailer)) {
            tailer.stop();
        }
        logDuplicateCheck.clear();
        readerSliceMap.clear();
    }

    private boolean skipNoMatchSchema(String schema, String logSchema) {
        return !Objects.equals(logSchema, schema);
    }

    private boolean skipNoInvalidSlice(SliceVo slice) {
        return Objects.isNull(slice) || StringUtils.isEmpty(slice.getTable());
    }
}
