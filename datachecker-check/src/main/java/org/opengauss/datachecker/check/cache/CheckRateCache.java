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

package org.opengauss.datachecker.check.cache;

import org.opengauss.datachecker.common.entry.check.CheckTable;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * CheckRateCache
 *
 * @author ：wangchao
 * @date ：Created in 2023/5/13
 * @since ：11
 */
@Service
public class CheckRateCache {
    private static final int MILLI_TO_SECOND = 1000;
    private static final int BYTE_TO_MB = 1024 * 1024;
    private static final int MAX_WINDOW_PERIOD = 30;
    private static final int WINDOW_PERIOD = 5;

    private Map<Long, List<CheckTable>> rateCache;
    private long total;
    private long totalRows;

    /**
     * init
     */
    public CheckRateCache() {
        rateCache = new LinkedHashMap<>(MAX_WINDOW_PERIOD, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Entry<Long, List<CheckTable>> eldest) {
                return size() > MAX_WINDOW_PERIOD;
            }
        };
    }

    /**
     * add checked table info
     *
     * @param table checked table info
     */
    public void add(CheckTable table) {
        if (rateCache.containsKey(table.getCompleteSecond())) {
            rateCache.get(table.getCompleteSecond()).add(table);
        } else {
            List<CheckTable> checkList = new LinkedList<>();
            checkList.add(table);
            rateCache.put(table.getCompleteSecond(), checkList);
        }
        total += table.getAvgRowLength() * table.getRowCount();
        totalRows += table.getRowCount();
    }

    /**
     * get current second check speed (MB/s)
     *
     * @return current speed
     */
    public int getCurrentSecondSpeed() {
        return getSpeed(System.currentTimeMillis());
    }

    /**
     * get avg check speed (MB/s)
     *
     * @param cost all cost time
     * @return avg speed
     */
    public int getAvgSpeed(long cost) {
        return (int) (total / (cost * BYTE_TO_MB));
    }

    /**
     * get complete check table size MB
     *
     * @return total
     */
    public long getTotal() {
        return total / BYTE_TO_MB;
    }

    /**
     * get complete check table rows
     *
     * @return totalRows
     */
    public long getTotalRows() {
        return totalRows;
    }

    private int getSpeed(long currentTimeMillis) {
        long[] windowPeriodTime = getWindowPeriodTime(currentTimeMillis);
        long windowPeriodSum = rateCache.entrySet().stream().filter(
            time -> time.getKey() >= windowPeriodTime[0] && time.getKey() < windowPeriodTime[1]).map(Entry::getValue)
                                        .map(secondInfo -> secondInfo.stream().map(
                                            table -> (table.getAvgRowLength() * table.getRowCount()))
                                                                     .mapToLong(Long::longValue).sum())
                                        .mapToLong(Long::longValue).sum();
        return (int) (windowPeriodSum / (WINDOW_PERIOD * BYTE_TO_MB));
    }

    private long[] getWindowPeriodTime(long currentTimeMillis) {
        long end = currentTimeMillis / MILLI_TO_SECOND;
        return new long[] {end - WINDOW_PERIOD, end};
    }
}
