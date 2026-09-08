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

package org.opengauss.datachecker.extract.resource;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.memory.JvmInfo;
import org.opengauss.datachecker.common.service.MemoryManager;
import org.opengauss.datachecker.common.service.ShutdownService;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.stereotype.Service;

import cn.hutool.core.thread.ThreadUtil;
import jakarta.annotation.Resource;

import java.math.BigDecimal;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Extract-side resource admission manager: caps concurrent slices with a connection quota
 * (aligned with Druid max-active) and admits new slices only when heap headroom allows,
 * to keep concurrent extraction from exhausting the heap.
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/25
 * @since ：11
 */
@Service
public class ResourceManager {
    private static final Logger log = LogUtils.getLogger(ResourceManager.class);

    // After this many consecutive admission failures, throttle with a wait to avoid busy spinning
    private static final int MAX_AVAILABLE_TIMES = 30;

    // Fallback physical connection cap (used when Druid config cannot be read)
    private static final int DEFAULT_MAX_CONNECTION_COUNT = 100;

    /**
     * Default headroom ratio: reserve this fraction of max heap unused, to absorb GC jitter
     * and short memory spikes. Actual value comes from spring.extract.memory-safe-watermark.
     */
    private static final double DEFAULT_MEMORY_HEADROOM_RATIO = 0.30;

    /**
     * Backpressure-tightened watermark: on queue full (Kafka sending blocked), the headroom ratio
     * is temporarily raised to this value to restrict new slice admission; running slices are
     * unaffected. Must not be too high: normal heap usage would then always exceed the threshold
     * and no new slice could ever be admitted.
     */
    private static final double TIGHTENED_MEMORY_WATERMARK = 0.40;

    /**
     * After this long without a new queue full event, the temporarily raised headroom
     * falls back to the configured value. Must not be too large: beyond the typical event
     * interval the relax timer keeps being reset and the fallback never happens.
     */
    private static final long WATERMARK_RELAX_AFTER_MS = 30000L;

    // Remaining connection quota (i.e. concurrent slice slots): decremented on admission, incremented on release()
    private volatile AtomicInteger connectionCount = new AtomicInteger(0);

    /**
     * Physical pool cap (read from Druid max-active at startup by initMaxConnectionCount());
     * used to cap connectionCount in release(), so a pairing bug cannot inflate the quota
     */
    private volatile int maxConnectionCap = DEFAULT_MAX_CONNECTION_COUNT;

    // Consecutive admission failures, used for throttling
    private volatile AtomicInteger tryAvailableTimes = new AtomicInteger(0);

    // Currently running slices: incremented on admission, decremented on release();
    // also used for the min-concurrency check when the heap watermark is exceeded
    private final AtomicInteger runningSlices = new AtomicInteger(0);

    // Safe watermark ratio (0~1), from spring.extract.memory-safe-watermark (default 0.30)
    private volatile double memorySafeWatermark = DEFAULT_MEMORY_HEADROOM_RATIO;

    // Configured watermark: the baseline for fallback. memorySafeWatermark may be temporarily
    // raised by backpressure and falls back to this
    private volatile double configuredMemorySafeWatermark = DEFAULT_MEMORY_HEADROOM_RATIO;

    // Timestamp of the last queue full (backpressure signal), used to decide whether to fall back
    private volatile long lastQueueFullAt = 0L;

    // Minimum concurrent slices: keep at least this many slices running even when the heap watermark
    // is exceeded, so an all-reject deadlock cannot stall progress (default 1, forced minimum 1)
    private volatile int memoryMinConcurrency = 1;

    // Whether admission parameters have been initialized from config
    private volatile boolean isAdmissionInitialized = false;

    @Resource
    private ShutdownService shutdownService;
    private ReentrantLock lock = new ReentrantLock();

    /**
     * Initialize the physical connection cap (aligned with Druid max-active) and load
     * memory admission parameters from config
     */
    public void initMaxConnectionCount() {
        int maxActive = ConfigCache.getIntValue(ConfigConstants.DRUID_MAX_ACTIVE);
        this.maxConnectionCap = maxActive > 0 ? maxActive : DEFAULT_MAX_CONNECTION_COUNT;
        connectionCount.set(this.maxConnectionCap);
        double watermark = ConfigCache.getDoubleValue(ConfigConstants.MEMORY_SAFE_WATERMARK, 0.30);
        // Out-of-range ratio (<=0 or >=1) is invalid; fall back to the default
        this.memorySafeWatermark = (watermark > 0 && watermark < 1) ? watermark : DEFAULT_MEMORY_HEADROOM_RATIO;
        this.configuredMemorySafeWatermark = this.memorySafeWatermark;
        // Force >=1 so tasks can always make progress under any heap pressure
        int minConcurrency = ConfigCache.getIntValue(ConfigConstants.MEMORY_MIN_CONCURRENCY, 1);
        this.memoryMinConcurrency = Math.max(1, minConcurrency);
        this.isAdmissionInitialized = true;
        final JvmInfo memory = MemoryManager.getJvmInfo();
        log.info("max active connection {} ,max memory {} ,safe-watermark {} ,min-concurrency {}",
            connectionCount.get(), memory.getMax(), memorySafeWatermark, memoryMinConcurrency);
    }

    /**
     * Get the parallel query degree queryDop
     *
     * @return queryDop
     */
    public int getParallelQueryDop() {
        return ConfigCache.getIntValue(ConfigConstants.QUERY_DOP);
    }

    /**
     * Get the remaining connection quota
     *
     * @return remaining connection quota
     */
    public int maxConnectionCount() {
        return connectionCount.get();
    }

    /**
     * Admission check: takes one connection quota slot and verifies heap headroom; grants only when both pass.
     * Returns false otherwise and the caller retries after waiting; under heap pressure, admission resumes
     * once running slices finish and GC frees the heap.
     *
     * @param freeSize estimated memory usage in bytes; <=0 means a small metadata query, skipping the memory check
     * @return true means a slot was taken and the caller MUST call release() to give it back
     */
    public boolean canExecQuery(long freeSize) {
        boolean isGranted;
        boolean isThrottled;
        lock.lock();
        try {
            maybeRelaxWatermark();
            isGranted = tryAdmitConnection(freeSize);
            isThrottled = !isGranted && tryAvailableTimes.incrementAndGet() >= MAX_AVAILABLE_TIMES;
        } finally {
            lock.unlock();
        }
        // Wait outside the lock: sleeping while holding it would block release() from
        // returning slots and extend the wait
        if (isThrottled) {
            ThreadUtil.safeSleep(500);
        }
        return isGranted;
    }

    /**
     * Called on a backpressure signal (queue full): temporarily raises the memory headroom to
     * restrict new slice admission; running slices are unaffected.
     * Automatically falls back to the configured value after a quiet period.
     */
    public void tightenWatermarkTemporarily() {
        if (memorySafeWatermark < TIGHTENED_MEMORY_WATERMARK) {
            memorySafeWatermark = TIGHTENED_MEMORY_WATERMARK;
            LogUtils.warn(log,
                "backpressure signal (queue full): tighten memory watermark {} -> {} , admission will be "
                    + "restricted until heap drains or no new signal for {}ms",
                configuredMemorySafeWatermark, TIGHTENED_MEMORY_WATERMARK, WATERMARK_RELAX_AFTER_MS);
        }
        // Reset the timer on every event: stay tightened while events keep coming,
        // start counting only after they calm down
        lastQueueFullAt = System.currentTimeMillis();
    }

    /**
     * Fallback check: after the headroom was raised by a backpressure signal, restore the
     * configured value once the quiet timeout elapses.
     * Invoked at the locked entry of canExecQuery, so no timer thread is needed.
     */
    private void maybeRelaxWatermark() {
        if (memorySafeWatermark > configuredMemorySafeWatermark
            && System.currentTimeMillis() - lastQueueFullAt > WATERMARK_RELAX_AFTER_MS) {
            memorySafeWatermark = configuredMemorySafeWatermark;
            LogUtils.info(log, "no backpressure signal for {}ms, relax memory watermark back to {}",
                WATERMARK_RELAX_AFTER_MS, configuredMemorySafeWatermark);
        }
    }

    /**
     * Try to take one connection quota slot and verify the memory watermark (with the min-concurrency exception).
     * Must be called while holding the lock.
     *
     * @param freeSize estimated memory usage in bytes; <=0 means a small metadata query, skipping the memory check
     * @return true means a slot was taken and the caller MUST call release() to give it back
     */
    private boolean tryAdmitConnection(long freeSize) {
        if (connectionCount.get() <= 0) {
            // Quota exhausted: wait for running slices to release() their slots
            LogUtils.info(log, "canExecQuery wait connection-count running={} connection={}",
                runningSlices.get(), connectionCount.get());
            return false;
        }
        // Heap watermark check; freeSize<=0 (small metadata query) only takes a connection, skipping the memory check
        boolean isHeapOk = freeSize <= 0 || hasFreeHeapMemory();
        // Min-concurrency exception: when the heap watermark is exceeded, still admit if running slices
        // are below memory-min-concurrency, so an all-reject deadlock cannot stall progress
        boolean isBypassMinConcurrency = !isHeapOk && runningSlices.get() < memoryMinConcurrency;
        if (!isHeapOk && !isBypassMinConcurrency) {
            // Heap watermark exceeded: wait for running slices to finish and GC to free the heap before admitting
            LogUtils.info(log,
                "canExecQuery wait real-heap-watermark running={} min-concurrency={} {} (requested={} bytes)",
                runningSlices.get(), memoryMinConcurrency, getJvmUsageString(), freeSize);
            return false;
        }
        connectionCount.decrementAndGet();
        runningSlices.incrementAndGet();
        tryAvailableTimes.set(0);
        LogUtils.info(log, "canExecQuery granted connection={} running={} used-heap-check={}",
            connectionCount.get(), runningSlices.get(),
            freeSize > 0 ? (isBypassMinConcurrency ? "min-concurrency-bypass" : getJvmUsageString())
                : "skip-small-query");
        return true;
    }

    /**
     * Check whether the heap watermark still has headroom: maxHeap - usedHeap > maxHeap x headroom ratio.
     * Reads real JVM values only; no estimate bookkeeping.
     *
     * @return true means heap usage is still below the safety line and admission is allowed
     */
    private boolean hasFreeHeapMemory() {
        Runtime rt = Runtime.getRuntime();
        long maxHeap = rt.maxMemory();
        long usedHeap = rt.totalMemory() - rt.freeMemory();
        long headroom = BigDecimal.valueOf(maxHeap).multiply(BigDecimal.valueOf(memorySafeWatermark)).longValue();
        return usedHeap <= maxHeap - headroom;
    }

    private String getJvmUsageString() {
        Runtime rt = Runtime.getRuntime();
        long maxHeap = rt.maxMemory();
        long usedHeap = rt.totalMemory() - rt.freeMemory();
        long headroom = BigDecimal.valueOf(maxHeap).multiply(BigDecimal.valueOf(memorySafeWatermark)).longValue();
        return String.format(Locale.ROOT, "used-heap=%d/%dMB headroom=%dMB assignable=%dMB",
            usedHeap / 1024 / 1024, maxHeap / 1024 / 1024, headroom / 1024 / 1024,
            (maxHeap - usedHeap - headroom) / 1024 / 1024);
    }

    /**
     * Take only a connection quota slot without the memory check (for small metadata queries)
     *
     * @return true means a slot was taken and the caller MUST call release() to give it back
     */
    public boolean canExecQuery() {
        lock.lock();
        try {
            maybeRelaxWatermark();
            if (connectionCount.get() > 0) {
                connectionCount.decrementAndGet();
                runningSlices.incrementAndGet();
                return true;
            }
        } finally {
            lock.unlock();
        }
        return false;
    }

    /**
     * Whether the service is shut down
     *
     * @return true if shut down
     */
    public boolean isShutdown() {
        return shutdownService.isShutdown();
    }

    /**
     * Return resources taken by admission: connection quota + concurrency counter; strictly paired
     * with canExecQuery() returning true.
     * Memory is not returned here (GC reclaims it). connectionCount is capped at the physical limit,
     * so a pairing bug cannot inflate the quota.
     */
    public void release() {
        lock.lock();
        try {
            int newCount = connectionCount.incrementAndGet();
            // Cap: if the caller releases more often than it admits, connectionCount would exceed the physical pool cap
            if (newCount > maxConnectionCap) {
                if (newCount - 1 < maxConnectionCap) {
                    LogUtils.warn(log,
                        "release() overflows connectionCount {} -> {}, capped at maxConnectionCap={}. "
                        + "Possible canExecQuery/release mismatch (more releases than admits).",
                        newCount - 1, newCount, maxConnectionCap);
                }
                connectionCount.set(maxConnectionCap);
            }
            // Return the concurrency counter, paired with the increment at admission
            int running = runningSlices.decrementAndGet();
            if (running < 0) {
                runningSlices.set(0);
                running = 0;
            }
            LogUtils.info(log, "release: corresponding connection returned, connection={}, running={}",
                connectionCount.get(), running);
        } finally {
            lock.unlock();
        }
    }
}
