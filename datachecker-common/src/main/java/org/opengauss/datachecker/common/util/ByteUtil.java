/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-tools-datachecker-performance is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.common.util;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class ByteUtil {
    private static final LongHashFunctionWrapper HASH_UTIL = new LongHashFunctionWrapper();

    /**
     * Compare whether the two byte arrays are consistent
     *
     * @param byte1 byte arrays
     * @param byte2 byte arrays
     * @return true|false
     */
    public static boolean isEqual(byte[] byte1, byte[] byte2) {
        if (byte1 == null || byte2 == null || byte1.length != byte2.length) {
            return false;
        }
        return HASH_UTIL.hashBytes(byte1) == HASH_UTIL.hashBytes(byte2);
    }

    /**
     * Convert a long number to a byte array
     *
     * @param value Long type number
     * @return byte array
     */
    public static byte[] toBytes(long value) {
        return new byte[] {(byte) (value >> 56), (byte) (value >> 48), (byte) (value >> 40), (byte) (value >> 32),
            (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value};
    }
}
