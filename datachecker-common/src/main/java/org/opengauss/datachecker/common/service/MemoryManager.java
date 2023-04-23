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

package org.opengauss.datachecker.common.service;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.memory.GcInfo;
import org.opengauss.datachecker.common.entry.memory.HeapInfo;
import org.opengauss.datachecker.common.entry.memory.MemoryInfo;
import org.opengauss.datachecker.common.entry.memory.OsInfo;
import org.opengauss.datachecker.common.entry.memory.ThreadInfo;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.util.List;

/**
 * MemoryManager
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/29
 * @since ：11
 */
@Slf4j
public class MemoryManager {
    /**
     * logging the runtimeInfo
     */
    public static void getRuntimeInfo() {
        log.info("{}{}{}{}{}{}", System.lineSeparator(), getOsMemoryInfo(), getJvmMemoryInfo(), getHeapMemoryInfo(),
            getThreadInfo(), getGcInfo());
    }

    /**
     * get MemoryInfo
     *
     * @return MemoryInfo
     */
    public static MemoryInfo getJvmMemoryInfo() {
        final Runtime runtime = Runtime.getRuntime();
        return new MemoryInfo(runtime.totalMemory(), runtime.freeMemory(), runtime.maxMemory());
    }

    private static ThreadInfo getThreadInfo() {
        final ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
        final int threadCount = threadMxBean.getThreadCount();
        final int peakThreadCount = threadMxBean.getPeakThreadCount();
        final int daemonThreadCount = threadMxBean.getDaemonThreadCount();
        return new ThreadInfo(threadCount, peakThreadCount, daemonThreadCount);
    }

    private static String getGcInfo() {
        final List<GarbageCollectorMXBean> gcList = ManagementFactory.getGarbageCollectorMXBeans();
        StringBuilder gcInfo = new StringBuilder();
        gcInfo.append("-------------------- JVM GC Information --------------------");
        gcInfo.append(System.lineSeparator());
        for (GarbageCollectorMXBean gc : gcList) {
            final String name = gc.getName();
            final long collectionTime = gc.getCollectionTime();
            final long collectionCount = gc.getCollectionCount();
            gcInfo.append(new GcInfo(name, collectionCount, collectionTime).toString());
            gcInfo.append(System.lineSeparator());
        }
        return gcInfo.toString();
    }

    private static HeapInfo getHeapMemoryInfo() {
        final MemoryMXBean memoryMxBean = ManagementFactory.getMemoryMXBean();
        final MemoryUsage memoryUsage = memoryMxBean.getHeapMemoryUsage();
        return new HeapInfo(memoryUsage.getInit(), memoryUsage.getMax(), memoryUsage.getUsed());
    }

    private static OsInfo getOsMemoryInfo() {
        final OperatingSystemMXBean systemMxBean = ManagementFactory.getOperatingSystemMXBean();
        final String osName = System.getProperty("os.name");
        final String version = systemMxBean.getVersion();
        final int availableProcessors = systemMxBean.getAvailableProcessors();
        final double systemLoadAverage = systemMxBean.getSystemLoadAverage();
        final String arch = systemMxBean.getArch();
        return new OsInfo(osName, version, availableProcessors, systemLoadAverage, arch);
    }
}
