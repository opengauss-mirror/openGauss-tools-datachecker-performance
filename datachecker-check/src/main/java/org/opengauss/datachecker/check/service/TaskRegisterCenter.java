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
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.MapUtils;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

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
    private static final Logger log = LogUtils.getLogger();
    protected static final AtomicInteger sliceTotalCount = new AtomicInteger();
    protected static final AtomicInteger tableCount = new AtomicInteger();

    protected static final Map<String, SliceVo> center = new ConcurrentHashMap<>();
    protected static final Map<String, Integer> sliceTableCounter = new ConcurrentHashMap<>();
    protected static final Map<String, Map<Endpoint, SliceExtend>> sliceExtendMap = new ConcurrentHashMap<>();
    private static final int status_updated_all = 3;
    private final ReentrantLock lock = new ReentrantLock();
    @Resource
    private SliceCheckEventHandler sliceCheckEventHandler;

    /**
     * register slice info to register center
     *
     * @param sliceVo slice
     */
    public void register(SliceVo sliceVo) {
        lock.lock();
        try {
            if (center.containsKey(sliceVo.getName())) {
                log.debug("{} register slice [{}] ", sliceVo.getEndpoint(), sliceVo.getName());
                return;
            }
            log.debug("{} register slice [{}] ", sliceVo.getEndpoint(), sliceVo.getName());
            center.put(sliceVo.getName(), sliceVo);
            addTableSliceCounter(sliceVo);
            log.info("register slice [{}] , current = [tableCount={},sliceTotal={},currentSize={}]", sliceVo.getName(),
                tableCount.get(), sliceTotalCount.get(), center.size());
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
                log.info("{} update slice [{}] status [{}->{}]", endpoint, sliceName, oldStatus, curStatus);
                if (curStatus == status_updated_all) {
                    notifySliceCheckHandle(slice);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void notifySliceCheckHandle(SliceVo slice) {
        String sliceName = slice.getName();
        SliceExtend source = MapUtils.get(sliceExtendMap, sliceName, Endpoint.SOURCE);
        SliceExtend sink = MapUtils.get(sliceExtendMap, sliceName, Endpoint.SINK);
        sliceCheckEventHandler.handle(new SliceCheckEvent(slice, source, sink));
        remove(slice);
    }

    public void remove(SliceVo sliceVo) {
        lock.lock();
        try {
            center.remove(sliceVo.getName());
            sliceExtendMap.remove(sliceVo.getName());
            log.info("drop slice [{}] due to had notified , release [{}]", sliceVo.getName(), center.size());
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
        return sliceTableCounter.values()
                                .stream()
                                .allMatch(count -> count == 0) && sliceTableCounter.size() == tableCount;
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
                tableReleaseSize--;
                sliceTableCounter.put(tableName, tableReleaseSize);
                log.debug("table [{}] slice release {}", tableName, tableReleaseSize);
            }
            return tableReleaseSize == 0;
        } finally {
            lock.unlock();
        }
    }
}
