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

import lombok.Setter;

import java.util.LinkedList;
import java.util.List;

/**
 * MemoryInfo
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/29
 * @since ：11
 */
@Setter
public class MemoryInfo extends BaseMonitor implements MonitorFormatter {
    private long vmTotal;
    private long vmFree;
    private long vmMax;
    private long vmUse;

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

    @Override
    public String toString() {
        return format();
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

    @Override
    public String getTitle() {
        return "Memory Information";
    }

    @Override
    public List<Field> getFormatFields() {
        List<Field> memoryFields = new LinkedList<>();
        memoryFields.add(Field.of("total", byteToMb(vmTotal) + " MB"));
        memoryFields.add(Field.of("maximum", byteToMb(vmMax) + " MB "));
        memoryFields.add(Field.of("used", byteToMb(vmUse) + " MB"));
        memoryFields.add(Field.of("free", byteToMb(vmFree) + " MB"));
        return memoryFields;
    }
}
