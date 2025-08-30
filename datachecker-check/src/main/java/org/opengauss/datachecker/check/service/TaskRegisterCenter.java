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

package org.opengauss.datachecker.check.service;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.slice.SliceCheckEvent;
import org.opengauss.datachecker.check.slice.SliceCheckEventHandler;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.MapUtils;
import org.springframework.stereotype.Component;

import jakarta.annotation.Resource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * TaskRegisterCenter
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/2
 * @since ：11
 */
@SuppressWarnings("AlibabaConstantFieldShouldBeUpperCase")
@Component
public class TaskRegisterCenter {
    protected static final AtomicInteger sliceTotalCount = new AtomicInteger();
    protected static final AtomicInteger tableCount = new AtomicInteger();
    protected static final Map<String, SliceVo> center = new ConcurrentHashMap<>();
    protected static final Map<String, Integer> sliceTableCounter = new ConcurrentHashMap<>();
    protected static final Map<String, Map<Endpoint, SliceExtend>> sliceExtendMap = new ConcurrentHashMap<>();

    private static final Logger log = LogUtils.getLogger(TaskRegisterCenter.class);
    private static final int STATUS_UPDATED_ALL = 3;
    private static final int STATUS_FAILED = -1;

    private final ReentrantLock lock = new ReentrantLock();
    @Resource
    private SliceCheckEventHandler sliceCheckEventHandler;
    private Map<String, String> sourceIgnoreMap = new HashMap<>();
    private Map<String, String> sinkIgnoreMap = new HashMap<>();
    private Map<String, Integer> tableSliceChangeMap = new ConcurrentHashMap<>();

    /**
     * register slice info to register center
     *
     * @param sliceVo slice
     */
    public void register(SliceVo sliceVo) {
        lock.lock();
        try {
            if (center.containsKey(sliceVo.getName())) {
                LogUtils.debug(log, "{} register slice [{}] ", sliceVo.getEndpoint(), sliceVo.getName());
                return;
            }
            LogUtils.debug(log, "{} register slice [{}] ", sliceVo.getEndpoint(), sliceVo.getName());
            center.put(sliceVo.getName(), sliceVo);
            addTableSliceCounter(sliceVo);
            LogUtils.info(log, "register slice [{}] , current = [tableCount={},sliceTotal={},currentSize={}]",
                sliceVo.getName(), tableCount.get(), sliceTotalCount.get(), center.size());
        } finally {
            lock.unlock();
        }
    }

    private void addTableSliceCounter(SliceVo sliceVo) {
        int total = sliceVo.getTotal();
        sliceTableCounter.compute(sliceVo.getTable(), (table, vTotal) -> {
            if (vTotal == null) {
                vTotal = total;
                tableCount.incrementAndGet();
                sliceTotalCount.addAndGet(total);
            }
            return vTotal;
        });
    }

    /**
     * update slice status ,and monitor status,that is equal status_updated_all .
     * if status == status_updated_all , build slice check event and notify slice check event handler.
     *
     * @param sliceExt sliceExt
     */
    public void update(SliceExtend sliceExt) {
        lock.lock();
        try {
            String sliceName = sliceExt.getName();
            if (center.containsKey(sliceName)) {
                SliceVo slice = center.get(sliceName);
                int oldStatus = slice.getStatus();
                slice.setStatus(sliceExt.getStatus());
                int curStatus = slice.getStatus();
                Endpoint endpoint = sliceExt.getEndpoint();
                MapUtils.put(sliceExtendMap, sliceName, endpoint, sliceExt);
                LogUtils.debug(log, "{} update slice [{}] status [{}->{}]", endpoint, sliceName, oldStatus, curStatus);
                SliceExtend source = MapUtils.get(sliceExtendMap, sliceName, Endpoint.SOURCE);
                SliceExtend sink = MapUtils.get(sliceExtendMap, sliceName, Endpoint.SINK);
                if (curStatus == STATUS_UPDATED_ALL) {
                    sliceCheckEventHandler.handle(new SliceCheckEvent(slice, source, sink));
                    remove(slice);
                } else if (curStatus == STATUS_FAILED) {
                    sliceCheckEventHandler.handleFailed(new SliceCheckEvent(slice, source, sink));
                    remove(slice);
                } else {
                    log.debug("slice data is extracting {}", slice.getName());
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void notifyIgnoreSliceCheckHandle(String table) {
        removeIgnoreTables(table);
        List<Entry<String, SliceVo>> tableSliceList = center.entrySet()
            .stream()
            .filter(entry -> entry.getValue().getTable().equals(table))
            .collect(Collectors.toList());
        Optional.of(tableSliceList).ifPresent(list -> {
            Entry<String, SliceVo> firstSlice = list.get(0);
            String sliceName = firstSlice.getValue().getName();
            SliceExtend source = MapUtils.get(sliceExtendMap, sliceName, Endpoint.SOURCE);
            SliceExtend sink = MapUtils.get(sliceExtendMap, sliceName, Endpoint.SINK);
            sliceCheckEventHandler.handleIgnoreTable(firstSlice.getValue(), source, sink);
            String csvDataPath = ConfigCache.getCsvData();
            list.forEach(entry -> {
                if (FileUtils.renameTo(csvDataPath, entry.getKey())) {
                    LogUtils.debug(log, "rename csv sharding completed [{}] by {}", entry.getKey(), "table miss");
                }
                remove(entry.getKey());
            });
        });
    }

    /**
     * remove center and sliceExtend cache
     *
     * @param sliceVo sliceVo
     */
    public void remove(SliceVo sliceVo) {
        remove(sliceVo.getName());
    }

    private void remove(String sliceName) {
        lock.lock();
        try {
            center.remove(sliceName);
            sliceExtendMap.remove(sliceName);
            LogUtils.info(log, "drop slice [{}] due to had notified , release [{}]", sliceName, center.size());
        } finally {
            lock.unlock();
        }
    }

    /**
     * refresh slice counter, if slice of table checked all ,then trigger clean table
     *
     * @param sliceVo slice
     */
    public boolean refreshAndCheckTableCompleted(SliceVo sliceVo) {
        return refreshCheckedTableCompleted(sliceVo.getTable());
    }

    /**
     * check table is all checked complete.
     *
     * @param tableCount table count
     * @return boolean true | false
     */
    public boolean checkCompletedAll(int tableCount) {
        // 处理csv场景已经忽略的表
        if (!(sourceIgnoreMap.isEmpty() && sinkIgnoreMap.isEmpty())) {
            sliceTableCounter.entrySet()
                .stream()
                .filter(tableEntry -> tableEntry.getValue() > 0 && (sourceIgnoreMap.containsKey(tableEntry.getKey())
                    || sinkIgnoreMap.containsKey(tableEntry.getKey())))
                .forEach(ignoreTable -> {
                    notifyIgnoreSliceCheckHandle(ignoreTable.getKey());
                    LogUtils.warn(log, "ignore table {} ===add ignore table to result===", ignoreTable.getKey());
                });
        }
        sliceTableCounter.entrySet().stream().filter(tableEntry -> tableEntry.getValue() > 0).forEach((entry) -> {
            if (tableSliceChangeMap.containsKey(entry.getKey())) {
                if (!Objects.equals(tableSliceChangeMap.get(entry.getKey()), entry.getValue())) {
                    tableSliceChangeMap.put(entry.getKey(), entry.getValue());
                    log.info("check complete release table {} slice {}", entry.getKey(), entry.getValue());
                }
                if (entry.getValue() == 0) {
                    tableSliceChangeMap.remove(entry.getKey());
                }
            } else {
                tableSliceChangeMap.put(entry.getKey(), entry.getValue());
                log.info("check complete release table {} slice {}", entry.getKey(), entry.getValue());
            }
        });
        return sliceTableCounter.values().stream().allMatch(count -> count == 0)
            && sliceTableCounter.size() == tableCount;
    }

    /**
     * refresh table checked complete.
     *
     * @param tableName table
     * @return boolean true|false
     */
    public boolean refreshCheckedTableCompleted(String tableName) {
        lock.lock();
        try {
            int tableReleaseSize = 0;
            if (sliceTableCounter.containsKey(tableName)) {
                tableReleaseSize = sliceTableCounter.get(tableName);
                if (tableReleaseSize > 0) {
                    tableReleaseSize--;
                    sliceTableCounter.put(tableName, tableReleaseSize);
                }
                LogUtils.debug(log, "table [{}] slice release {}", tableName, tableReleaseSize);
            }
            return tableReleaseSize == 0;
        } finally {
            lock.unlock();
        }
    }

    /**
     * addCheckIgnoreTable
     *
     * @param endpoint endpoint
     * @param table table
     * @param reason reason
     */
    public void addCheckIgnoreTable(Endpoint endpoint, String table, String reason) {
        if (Objects.equals(endpoint, Endpoint.SOURCE)) {
            sourceIgnoreMap.put(table, reason);
        } else {
            sinkIgnoreMap.put(table, reason);
        }
        refreshIgnoreTables(table);
    }

    private void refreshIgnoreTables(String table) {
        if (sourceIgnoreMap.containsKey(table) && sinkIgnoreMap.containsKey(table)) {
            sourceIgnoreMap.remove(table);
            sinkIgnoreMap.remove(table);
        }
    }

    private void removeIgnoreTables(String table) {
        if (sourceIgnoreMap.containsKey(table)) {
            sourceIgnoreMap.remove(table);
        }
        if (sinkIgnoreMap.containsKey(table)) {
            sinkIgnoreMap.remove(table);
        }
    }
}
