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
 * MemoryBase
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/29
 * @since ：11
 */
public class MemoryBase {
    private static final int BYTE_TO_MB = 1024 * 1024;

    /**
     * byte to Mb
     *
     * @param value byte value
     * @return mb
     */
    protected static long byteToMb(long value) {
        return value / BYTE_TO_MB;
    }
}
