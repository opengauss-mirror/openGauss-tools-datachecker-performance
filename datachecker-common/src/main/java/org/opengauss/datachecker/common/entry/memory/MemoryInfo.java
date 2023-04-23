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
 * MemoryInfo
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/29
 * @since ：11
 */
public class MemoryInfo extends MemoryBase {
    private long vmTotal;
    private long vmFree;
    private long vmMax;
    private long vmUse;

    public MemoryInfo(long vmTotal, long vmFree, long vmMax) {
        this.vmTotal = vmTotal;
        this.vmFree = vmFree;
        this.vmMax = vmMax;
        this.vmUse = vmTotal - vmFree;
    }

    public long getVmTotal() {
        return byteToMb(vmTotal);
    }

    public long getVmFree() {
        return byteToMb(vmFree);
    }

    public long getVmMax() {
        return byteToMb(vmMax);
    }

    public long getVmUse() {
        return byteToMb(vmUse);
    }

    /**
     * format template
     * <p>
     * -------------------- JVM Memory Information --------------------
     * JVM Memory used: vmUse,  Free Memory : vmFree, Total Memory: vmTotal, Maximum Memory: vmMax
     * </p>
     *
     * @return format
     */
    @Override
    public String toString() {
        return "-------------------- JVM Memory Information --------------------" + System.lineSeparator()
            + "| JVM Memory used: " + byteToMb(vmUse) + " MB, Free Memory: " + byteToMb(vmFree) + " MB, Total Memory: "
            + byteToMb(vmTotal) + " MB, Maximum Memory: " + byteToMb(vmMax) + " MB " + System.lineSeparator();
    }

    /**
     * free memory is available
     *
     * @param freeSize free memory
     * @return isAvailable
     */
    public boolean isAvailable(long freeSize) {
        return vmFree >= freeSize;
    }
}
