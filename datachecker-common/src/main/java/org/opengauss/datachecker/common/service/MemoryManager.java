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
import org.opengauss.datachecker.common.entry.memory.CpuInfo;
import org.opengauss.datachecker.common.entry.memory.GcInfo;
import org.opengauss.datachecker.common.entry.memory.JvmInfo;
import org.opengauss.datachecker.common.entry.memory.MemoryInfo;
import org.opengauss.datachecker.common.entry.memory.OsInfo;
import org.opengauss.datachecker.common.entry.memory.ThreadInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.CentralProcessor.TickType;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.util.Util;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Properties;

/**
 * MemoryManager
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/29
 * @since ：11
 */
@Slf4j
public class MemoryManager {
    private static final int OSHI_WAIT_MILLIS = 500;
    private static final OsInfo OS_INFO = getOsInfo();
    private static final CpuInfo CPU = new CpuInfo();
    private static final MemoryInfo MEM = new MemoryInfo();
    private static final JvmInfo JVM = new JvmInfo();
    private static final ThreadInfo THREAD = new ThreadInfo();

    /**
     * logging the runtimeInfo
     *
     * @param hardware hardware
     */
    public static void getRuntimeInfo(HardwareAbstractionLayer hardware) {
        setMemInfo(hardware.getMemory());
        setJvmInfo();
        setThreadInfo();
        log.info("{}{}{}{}{}{}{}", System.lineSeparator(), OS_INFO, CPU.toString(), MEM.toString(), JVM.toString(),
            THREAD.toString(), getGcInfo());
    }

    private static void setJvmInfo() {
        Properties props = System.getProperties();
        JVM.setTotal(Runtime.getRuntime().totalMemory());
        JVM.setMax(Runtime.getRuntime().maxMemory());
        JVM.setFree(Runtime.getRuntime().freeMemory());
        JVM.setVersion(props.getProperty("java.version"));
        JVM.setHome(props.getProperty("java.home"));
    }

    /**
     * JvmInfo
     *
     * @return JvmInfo
     */
    public static JvmInfo getJvmInfo() {
        setJvmInfo();
        return JVM;
    }

    private static void setMemInfo(GlobalMemory memory) {
        MEM.setVmTotal(memory.getTotal());
        MEM.setVmUse(memory.getTotal() - memory.getAvailable());
        MEM.setVmFree(memory.getAvailable());
    }

    private static void setThreadInfo() {
        final ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();
        THREAD.setThreadCount(threadMxBean.getThreadCount());
        THREAD.setPeakThreadCount(threadMxBean.getPeakThreadCount());
        THREAD.setDaemonThreadCount(threadMxBean.getDaemonThreadCount());
    }

    private static String getGcInfo() {
        final List<GarbageCollectorMXBean> gcList = ManagementFactory.getGarbageCollectorMXBeans();
        StringBuilder gcInfo = new StringBuilder();
        for (GarbageCollectorMXBean gc : gcList) {
            final String name = gc.getName();
            final long collectionTime = gc.getCollectionTime();
            final long collectionCount = gc.getCollectionCount();
            gcInfo.append(new GcInfo(name, collectionCount, collectionTime).toString());
        }
        return gcInfo.toString();
    }

    private static OsInfo getOsInfo() {
        final OperatingSystemMXBean systemMxBean = ManagementFactory.getOperatingSystemMXBean();
        final String osName = System.getProperty("os.name");
        final String version = systemMxBean.getVersion();
        final int availableProcessors = systemMxBean.getAvailableProcessors();
        final double systemLoadAverage = systemMxBean.getSystemLoadAverage();
        final String arch = systemMxBean.getArch();
        return new OsInfo(osName, version, availableProcessors, systemLoadAverage, arch);
    }

    /**
     * get CpuInfo
     *
     * @return CpuInfo
     */
    public static CpuInfo getCpuInfo() {
        return CPU;
    }

    /**
     * set cpu processor info
     *
     * @param processor processor
     */
    public static void setCpuInfo(CentralProcessor processor) {
        long[] prevTicks = processor.getSystemCpuLoadTicks();
        // must wait {@value OSHI_WAIT_SECOND} millis
        Util.sleep(OSHI_WAIT_MILLIS);
        long[] ticks = processor.getSystemCpuLoadTicks();

        long nice = ticks[TickType.NICE.getIndex()] - prevTicks[TickType.NICE.getIndex()];
        long irq = ticks[TickType.IRQ.getIndex()] - prevTicks[TickType.IRQ.getIndex()];
        long softirq = ticks[TickType.SOFTIRQ.getIndex()] - prevTicks[TickType.SOFTIRQ.getIndex()];
        long steal = ticks[TickType.STEAL.getIndex()] - prevTicks[TickType.STEAL.getIndex()];
        long cSys = ticks[TickType.SYSTEM.getIndex()] - prevTicks[TickType.SYSTEM.getIndex()];
        long user = ticks[TickType.USER.getIndex()] - prevTicks[TickType.USER.getIndex()];
        long iowait = ticks[TickType.IOWAIT.getIndex()] - prevTicks[TickType.IOWAIT.getIndex()];
        long idle = ticks[TickType.IDLE.getIndex()] - prevTicks[TickType.IDLE.getIndex()];
        long totalCpu = user + nice + cSys + idle + iowait + irq + softirq + steal;

        CPU.setCpuNum(processor.getLogicalProcessorCount());
        CPU.setTotal(totalCpu);
        CPU.setSys(cSys);
        CPU.setUsed(user);
        CPU.setWait(iowait);
        CPU.setFree(idle);
    }
}
