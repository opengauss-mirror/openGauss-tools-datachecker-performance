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

package org.opengauss.datachecker.common.util;

import net.openhft.hashing.LongHashFunction;
import org.springframework.lang.NonNull;

import java.nio.charset.Charset;

/**
 * LongHashFunctionWrapper
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/24
 * @since ：11
 */
public class LongHashFunctionWrapper {
    private static final long XX3_SEED = 199972221018L;

    /**
     * hashing algorithm
     */
    private static final LongHashFunction XX_3_HASH = LongHashFunction.xx3(XX3_SEED);

    /**
     * Hash the byte array using the XX3 hash algorithm
     *
     * @param input byte array
     * @return Hash value
     */
    public long hashBytes(@NonNull byte[] input) {
        return XX_3_HASH.hashBytes(input);
    }

    /**
     * Hash the string using the XX3 hash algorithm
     *
     * @param input string
     * @return Hash value
     */
    public long hashBytes(@NonNull String input) {
        return XX_3_HASH.hashBytes(input.getBytes(Charset.defaultCharset()));
    }
}
