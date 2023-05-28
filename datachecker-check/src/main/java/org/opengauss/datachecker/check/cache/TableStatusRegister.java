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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.opengauss.datachecker.common.entry.check.CheckProgress;
import org.opengauss.datachecker.common.entry.check.Pair;
import org.opengauss.datachecker.common.exception.ExtractException;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.stereotype.Service;

import javax.validation.constraints.NotEmpty;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
@Slf4j
@Service
public class TableStatusRegister implements Cache<String, Integer> {
    /**
     * Task status: if both the source and destination tasks complete data extraction, the setting status is 3
     */
    public static final int TASK_STATUS_COMPLETED_VALUE = 3;
    /**
     * Task status: if table data verification the current table cache status will be updated to value = value | 4
     */
    public static final int TASK_STATUS_CHECK_VALUE = 4;
    /**
     * Task status: if both the source and destination tasks have completed data verification, the setting status is 7
     */
    public static final int TASK_STATUS_CONSUMER_VALUE = 7;
    /**
     * Task status cache. the table of data extract has error.
     */
    public static final int TASK_STATUS_ERROR = -1;
    /**
     * Task status cache. The initial default value of status is 0
     */
    public static final int TASK_STATUS_DEFAULT_VALUE = 0;
    /**
     * Status self check thread name
     */
    private static final String SELF_CHECK_THREAD_NAME = "task-status-manager";
    private static final AtomicInteger CHECK_COUNT = new AtomicInteger(0);
    /**
     * <pre>
     * Data extraction task execution state cache
     * {@code tableStatusCache} : key Name of data extraction table
     * {@code tableStatusCache} : value Data extraction table completion status
     * value  initialization status is 0
     * If the source endpoint completes the table identification as 1,
     * the current table cache status will be updated as value = value | 1
     * If the sink endpoint completes the table identification as 2,
     * the current table cache status will be updated as value = value | 2
     * If the data verification ID is 4, the current table cache status will be updated to value = value | 4
     * </pre>
     */
    private static final Map<String, Integer> TABLE_STATUS_CACHE = new ConcurrentHashMap<>();
    private static final Map<String, Map<Integer, Integer>> TABLE_PARTITIONS_STATUS_CACHE = new ConcurrentHashMap<>();

    /**
     * complete
     */
    private static final BlockingDeque<String> COMPLETED_TABLE_QUEUE = new LinkedBlockingDeque<>();

    /**
     * The service starts to recover cached information. Recover historical data based on persistent cached data
     * Scan the cache file at the specified location, parse the JSON string, and deserialize the current cache data
     */
    @Override
    public void recover() {
        // Scan the cache file at the specified location, parse the JSON string, and deserialize the current cache data
    }

    /**
     * Start and execute self-test thread
     */
    public void selfCheck() {
        ScheduledExecutorService scheduledExecutor = ThreadUtil.newSingleThreadScheduledExecutor();
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            Thread.currentThread().setName(SELF_CHECK_THREAD_NAME);
            doCheckingStatus();
        }, 5, 1, TimeUnit.SECONDS);
    }

    /**
     * When the number of completed data extraction tasks is consistent with the number of consumed completed tasks,
     * and is greater than 0, the verification service is considered to have been completed
     *
     * @return boolean
     */
    public boolean isCheckCompleted() {
        return TABLE_STATUS_CACHE.values().stream().filter(status -> status >= TASK_STATUS_DEFAULT_VALUE)
                                 .allMatch(status -> status == TASK_STATUS_CONSUMER_VALUE);
    }

    /**
     * calc checked table
     *
     * @return checed table count
     */
    public int getCheckedCount() {
        return errorCount() + checkCount();
    }

    /**
     * View the overall completion status of all extraction tasks at the source and destination
     *
     * @return All complete - returns true
     */
    public boolean isExtractCompleted() {
        return extractCompletedCount() == cacheSize();
    }

    /**
     * Task status reset
     */
    public void rest() {
        init(TABLE_STATUS_CACHE.keySet());
        TABLE_PARTITIONS_STATUS_CACHE.clear();
        COMPLETED_TABLE_QUEUE.clear();
    }

    /**
     * cache is empty
     *
     * @return cache is empty
     */
    public boolean isEmpty() {
        return TABLE_STATUS_CACHE.isEmpty();
    }

    /**
     * task has extract completed
     *
     * @return task has extract completed
     */
    public boolean hasExtractCompleted() {
        return extractCompletedCount() > 0;
    }

    /**
     * task has extract completed count
     *
     * @return task has extract completed count
     */
    private int extractCompletedCount() {
        return (int) TABLE_STATUS_CACHE.values().stream().filter(status -> status >= TASK_STATUS_COMPLETED_VALUE)
                                       .count();
    }

    /**
     * table has check completed count
     *
     * @return table has check completed count
     */
    public int checkCompletedCount() {
        return (int) TABLE_STATUS_CACHE.values().stream().filter(status -> status >= TASK_STATUS_CONSUMER_VALUE)
                                       .count();
    }

    /**
     * extract progress
     *
     * @return extract progress
     */
    public CheckProgress extractProgress() {
        return new CheckProgress(errorCount(), extractingCount(), extractCount(), checkCount());
    }

    /**
     * check progress
     *
     * @return check progress
     */
    public Pair<Integer, Integer> checkProgress() {
        return Pair.of(checkCompletedCount(), cacheSize());
    }

    /**
     * Initialize cache and set default values for key values
     *
     * @param keys keys
     */
    @Override
    public void init(@NotEmpty Set<String> keys) {
        keys.forEach(key -> {
            TABLE_STATUS_CACHE.put(key, TASK_STATUS_DEFAULT_VALUE);
        });
    }

    /**
     * Add table state pairs to cache
     *
     * @param key   key
     * @param value value
     */
    @Override
    public void put(String key, Integer value) {
        if (TABLE_STATUS_CACHE.containsKey(key)) {
            // The current key already exists and cannot be added repeatedly
            throw new ExtractException("The current key= " + key + " already exists and cannot be added repeatedly");
        }
        TABLE_STATUS_CACHE.put(key, value);
    }

    /**
     * table of partitions status
     *
     * @param key        table name
     * @param partitions partitions
     */
    public void initPartitionsStatus(String key, Integer partitions) {
        if (!TABLE_STATUS_CACHE.containsKey(key)) {
            // The current key already exists and cannot be added repeatedly
            throw new ExtractException("The current key= " + key + " already exists and cannot be added repeatedly");
        }
        Map<Integer, Integer> partitionMap = new ConcurrentHashMap<>(InitialCapacity.CAPACITY_16);
        IntStream.range(0, partitions).forEach(partition -> {
            partitionMap.put(partition, TASK_STATUS_COMPLETED_VALUE);
        });
        TABLE_PARTITIONS_STATUS_CACHE.put(key, partitionMap);
    }

    /**
     * Update cached data
     *
     * @param key   key
     * @param value value
     * @return Updated cache value
     */
    @Override
    public synchronized Integer update(String key, Integer value) {
        if (!TABLE_STATUS_CACHE.containsKey(key)) {
            log.error("current key={} does not exist", key);
            return 0;
        }
        Integer odlValue = TABLE_STATUS_CACHE.get(key);
        TABLE_STATUS_CACHE.put(key, odlValue | value);
        final Integer status = TABLE_STATUS_CACHE.get(key);
        log.debug("update table[{}] status : {} -> {}", key, odlValue, status);
        if (status == TASK_STATUS_COMPLETED_VALUE) {
            putLast(key);
            log.debug("add table[{}] queue last", key);
        }
        return status;
    }

    /**
     * Update the current table corresponding to the Kafka partition data extraction status
     *
     * @param key       table
     * @param partition partition
     * @param value     status
     */
    public void update(String key, Integer partition, Integer value) {
        if (!TABLE_PARTITIONS_STATUS_CACHE.containsKey(key)) {
            log.error("current partition key={}  does not exist", key);
            return;
        }
        TABLE_PARTITIONS_STATUS_CACHE.get(key).put(partition, TASK_STATUS_COMPLETED_VALUE | value);
        log.debug("update table [{}] partition[{}] status : {}", key, partition, TASK_STATUS_CONSUMER_VALUE);
        boolean isAllCompleted = TABLE_PARTITIONS_STATUS_CACHE.get(key).values().stream()
                                                              .allMatch(status -> status == TASK_STATUS_CONSUMER_VALUE);
        if (isAllCompleted) {
            update(key, TASK_STATUS_CHECK_VALUE);
        }
    }

    private void putLast(String key) {
        try {
            COMPLETED_TABLE_QUEUE.putLast(key);
        } catch (InterruptedException e) {
            log.error("put key={} queue COMPLETED_TABLE_QUEUE error", key);
        }
    }

    /**
     * Query cache according to key
     *
     * @param key key
     * @return cache value
     */
    @Override
    public Integer get(String key) {
        return TABLE_STATUS_CACHE.getOrDefault(key, -1);
    }

    /**
     * query all table status
     *
     * @return table status
     */
    public Map<String, Integer> get() {
        return Collections.unmodifiableMap(TABLE_STATUS_CACHE);
    }

    /**
     * query all table partitions status
     *
     * @return table status
     */
    public Map<String, Map<Integer, Integer>> getTablePartitionsStatusCache() {
        return Collections.unmodifiableMap(TABLE_PARTITIONS_STATUS_CACHE);
    }

    /**
     * Get cache key set
     *
     * @return Key set
     */
    @Override
    public Set<String> getKeys() {
        return TABLE_STATUS_CACHE.keySet();
    }

    /**
     * cache size
     *
     * @return cache size
     */
    public Integer cacheSize() {
        return TABLE_STATUS_CACHE.keySet().size();
    }

    /**
     * Delete the specified key cache
     *
     * @param key key
     */
    @Override
    public void remove(String key) {
        TABLE_STATUS_CACHE.remove(key);
    }

    /**
     * Clear all caches
     */
    @Override
    public void removeAll() {
        TABLE_STATUS_CACHE.clear();
        COMPLETED_TABLE_QUEUE.clear();
        TABLE_PARTITIONS_STATUS_CACHE.clear();
        log.info("table status register cache information clearing");
    }

    /**
     * The cache persistence interface will persist the cache information locally
     * Persist the cache information to the local cache file, serialize it into JSON string,
     * and save it to the local specified file
     */
    @Override
    public void persistent() {
    }

    /**
     * Return and delete the statistical queue {@code completed_table_queue} header node
     * that has completed the data extraction task,
     * If the queue is empty, null is returned
     *
     * @return Return the queue header node. If the queue is empty, return null
     */
    public String completedTablePoll() {
        return COMPLETED_TABLE_QUEUE.poll();
    }

    /**
     * Check whether there is a completed data extraction task. If yes, update completed_ Table table
     * Check whether there is a completed data verification task. If yes, update consumer_ COMPLETED_ Table table
     *
     * @return check table count
     */
    private int doCheckingStatus() {
        Set<String> keys = TABLE_STATUS_CACHE.keySet();
        if (keys.size() <= 0) {
            return 0;
        }
        List<String> extractErrorList = new ArrayList<>();
        List<String> notExtractCompleteList = new ArrayList<>();
        List<String> notCheckCompleteList = new ArrayList<>();
        List<String> checkCompleteList = new ArrayList<>();
        keys.forEach(tableName -> {
            Integer status = get(tableName);
            if (status <= TASK_STATUS_ERROR) {
                extractErrorList.add(tableName);
            } else if (status < TASK_STATUS_COMPLETED_VALUE) {
                notExtractCompleteList.add(tableName);
            } else if (status == TASK_STATUS_COMPLETED_VALUE) {
                notCheckCompleteList.add(tableName);
            } else if (status == TASK_STATUS_CONSUMER_VALUE) {
                checkCompleteList.add(tableName);
            } else {
                log.debug("process check status running");
            }
        });
        List<String> tableStatus =
            TABLE_STATUS_CACHE.entrySet().stream().filter(entry -> entry.getValue() != 7).limit(10)
                              .map(entry -> entry.getKey().concat("=").concat(entry.getValue() + ""))
                              .collect(Collectors.toList());
        if(CollectionUtils.isNotEmpty(tableStatus)){
            log.debug("table_status :{} ", tableStatus);
        }
        return CHECK_COUNT.get();
    }

    private int errorCount() {
        return (int) TABLE_STATUS_CACHE.values().stream().filter(status -> status < TASK_STATUS_DEFAULT_VALUE).count();
    }

    private int extractingCount() {
        return (int) TABLE_STATUS_CACHE.values().stream().filter(
            status -> status >= TASK_STATUS_DEFAULT_VALUE && status < TASK_STATUS_COMPLETED_VALUE).count();
    }

    private int extractCount() {
        return (int) TABLE_STATUS_CACHE.values().stream().filter(status -> status == TASK_STATUS_COMPLETED_VALUE)
                                       .count();
    }

    private int checkCount() {
        return (int) TABLE_STATUS_CACHE.values().stream().filter(status -> status == TASK_STATUS_CONSUMER_VALUE)
                                       .count();
    }
}
