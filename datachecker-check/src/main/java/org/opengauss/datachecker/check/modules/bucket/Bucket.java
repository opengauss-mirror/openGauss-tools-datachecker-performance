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

package org.opengauss.datachecker.check.modules.bucket;

import lombok.Data;
import lombok.experimental.Accessors;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.util.ByteUtil;

import jakarta.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Data
@Accessors(chain = true)
public class Bucket implements Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * <pre>
     * Bucket initialization capacity.
     * If the amount of data in the bucket exceeds the specified amount {@code initialCapacity*0.75},
     * the bucket capacity expansion will be automatically triggered.
     * </pre>
     */
    private int initialCapacity;

    /**
     * <pre>
     * The initialization capacity of the bucket container is set to the average capacity.
     * <p>
     * If the average capacity is exceeded, the capacity will be expanded
     * </pre>
     */
    private Map<String, RowDataHash> bucket = new ConcurrentHashMap<>(initialCapacity);
    /**
     * bucket number
     */
    private Integer number;

    private Integer bucketCount;
    /**
     * Hash signature of bucket bucket. The initialization value of the signature is 1
     */
    private long signature = 1L;

    /**
     * Capacity initialization is required during barrel construction
     *
     * @param initialCapacity Bucket initialization capacity size
     */
    public Bucket(int initialCapacity) {
        this.initialCapacity = initialCapacity;
    }

    /**
     * Add the row record hash object to the bucket container. And calculate the hash signature of the bucket.
     * <p>
     * The hash signature algorithm of the bucket is the hash signature {@code signature}
     * of the current bucket or the row hash value of the currently inserted record.
     *
     * @param rowDataHash Row record hash object
     * @return Return Insert Collection Results
     */
    public RowDataHash put(@NotNull RowDataHash rowDataHash) {
        signature = signature ^ rowDataHash.getVHash();
        return bucket.put(rowDataHash.getKey(), rowDataHash);
    }

    /**
     * Get the hash signature of the current bucket
     *
     * @return Hash signature of bucket
     */
    public byte[] getSignature() {
        return ByteUtil.toBytes(signature);
    }

    public int getBucketCount() {
        bucketCount = bucket.size();
        return bucketCount;
    }

    @Override
    public String toString() {
        return "Bucket{ number=" + number + ", count=" + getBucketCount() + ", signature=" + signature + ", bucket="
            + bucket + '}';
    }
}
