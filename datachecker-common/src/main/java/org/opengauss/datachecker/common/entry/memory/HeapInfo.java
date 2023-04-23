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
 * HeapInfo
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/29
 * @since ：11
 */
public class HeapInfo extends MemoryBase {
    private long init;
    private long max;
    private long use;

    public HeapInfo(long init, long max, long use) {
        this.init = init;
        this.max = max;
        this.use = use;
    }

    /**
     * format template
     * <p>
     * -------------------- JVM Heap Memory Information --------------------
     * JVM Heap memory used: use, Initial heap size: init, Maximum heap memory:max
     * </p>
     *
     * @return format
     */
    @Override
    public String toString() {
        return "-------------------- JVM Heap Memory Information --------------------" + System.lineSeparator()
            + "| JVM Heap memory used: " + byteToMb(use) + " MB, Initial heap size: " + byteToMb(init)
            + " MB, Maximum heap memory:" + byteToMb(max) + " MB " + System.lineSeparator();
    }
}
