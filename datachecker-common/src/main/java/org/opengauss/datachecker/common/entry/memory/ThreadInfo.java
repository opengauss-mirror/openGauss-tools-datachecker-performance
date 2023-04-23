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

package org.opengauss.datachecker.common.entry.memory;

/**
 * ThreadInfo
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/29
 * @since ：11
 */
public class ThreadInfo {
    int threadCount;
    int peakThreadCount;
    int daemonThreadCount;

    /**
     * ThreadInfo
     *
     * @param threadCount       threadCount
     * @param peakThreadCount   peakThreadCount
     * @param daemonThreadCount daemonThreadCount
     */
    public ThreadInfo(int threadCount, int peakThreadCount, int daemonThreadCount) {
        this.threadCount = threadCount;
        this.peakThreadCount = peakThreadCount;
        this.daemonThreadCount = daemonThreadCount;
    }

    /**
     * format template
     * <p>
     * -------------------- JVM Thread Information --------------------
     * Number of threads: threadCount  Max threads: peakThreadCount  Number of daemon threads: daemonThreadCount
     * </p>
     *
     * @return format
     */
    @Override
    public String toString() {
        return "-------------------- JVM Thread Information --------------------" + System.lineSeparator()
            + "| Threads: " + threadCount + ", Max Threads: " + peakThreadCount + ", Daemon Threads: "
            + daemonThreadCount + System.lineSeparator();
    }
}
