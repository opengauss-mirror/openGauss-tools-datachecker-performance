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

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.springframework.lang.NonNull;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * BuilderBucketHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/24
 * @since ：11
 */
public class BuilderBucketHandler {
    /**
     * Maximum height of Merkel tree
     */
    private static final int MERKLE_TREE_MAX_HEIGHT = 15;
    /**
     * Maximum number of leaf nodes of the highest Merkel tree
     */
    private static final int BUCKET_MAX_COUNT_LIMITS = 1 << MERKLE_TREE_MAX_HEIGHT;

    /**
     * <pre>
     * When the maximum height of Merkel tree is limited to {@value MERKLE_TREE_MAX_HEIGHT}，
     * Then the maximum number of leaf nodes of the highest Merkel tree constructed is {@code BUCKET_MAX_COUNT_LIMITS},
     * that is {@value BUCKET_MAX_COUNT_LIMITS}。
     * <p>
     * Thus, the maximum number of barrels obtained is {@value BUCKET_MAX_COUNT_LIMITS }，
     * Range of barrels we limit the number of barrels per tree to 2^n
     * </pre>
     */
    private static final int[] BUCKET_COUNT_LIMITS = new int[MERKLE_TREE_MAX_HEIGHT];

    // initialize {@code BUCKET_COUNT_LIMITS}
    static {
        for (int i = 1; i <= MERKLE_TREE_MAX_HEIGHT; i++) {
            BUCKET_COUNT_LIMITS[i - 1] = 1 << i;
        }
    }

    /**
     * The capacity of empty barrels is used to construct special empty barrels
     */
    private static final int EMPTY_INITIAL_CAPACITY = 0;

    /**
     * Current bucket initialization capacity
     */
    private final int bucketCapacity;

    public BuilderBucketHandler(int bucketCapacity) {
        this.bucketCapacity = bucketCapacity;
    }

    /**
     * <pre>
     * Dynamically allocate {@code rowDataHashList} data to
     * bucket {@link org.opengauss.datachecker.check.modules.bucket.Bucket}.
     * </pre>
     *
     * @param rowDataHashList Collection of records currently to be allocated to the bucket
     * @param totalCount      Record the total number for {@link RowDataHash}.
     *                        Note: not necessarily the current {@code rowDataHashList.size} total
     * @param bucketMap       {@code bucketMap<K,V>} K为当前桶V的编号。
     */
    public void builder(@NonNull List<RowDataHash> rowDataHashList, int totalCount,
        @NonNull Map<Integer, Bucket> bucketMap) {
        // Calculate the current maximum number of barrels according to the total number of current records
        int maxBucketCount = calculateMaxBucketCount(totalCount);
        // Average bucket capacity - used to initialize the bucket capacity size
        int averageCapacity = totalCount / maxBucketCount;
        rowDataHashList.forEach(row -> {
            long primaryKeyHash = row.getKHash();
            // Calculate bucket number information
            Integer bucketNumber = calculateBucketNumber(primaryKeyHash, maxBucketCount);
            // Obtain the bucket with the specified number according to the row information,
            // and create the bucket if it does not exist
            if (!bucketMap.containsKey(bucketNumber)) {
                bucketMap.put(bucketNumber, new Bucket(averageCapacity).setNumber(bucketNumber));
            }
            // Add row to the bucket with the specified bucket number
            bucketMap.get(bucketNumber).put(row);
        });
    }

    /**
     * <pre>
     * Calculate the current maximum number of barrels according to the total number of {@code totalCount} records.
     * The number of barrels is 2^n
     * </pre>
     *
     * @param totalCount Total records
     * @return Maximum barrels
     */
    private int calculateMaxBucketCount(int totalCount) {
        int bucketCount = totalCount / bucketCapacity;
        int asInt = IntStream.range(0, 15).filter(idx -> BUCKET_COUNT_LIMITS[idx] > bucketCount).findFirst().orElse(14);
        return BUCKET_COUNT_LIMITS[asInt];
    }

    /**
     * Mark the current record according to the {@code rowHash} value, which is used for the number of barrels
     *
     * @param primaryKeyHash Row record primary key hash value
     * @param bucketCount    Number of barrels the number of barrels is 2^n
     * @return Line record bucket number
     */
    private int calculateBucketNumber(long primaryKeyHash, int bucketCount) {
        return (int) (Math.abs(primaryKeyHash) & (bucketCount - 1));
    }

    /**
     * Construct empty barrels according to the number
     *
     * @param bucketNumber bucket number
     * @return bucket
     */
    public static Bucket builderEmpty(Integer bucketNumber) {
        return new Bucket(EMPTY_INITIAL_CAPACITY).setNumber(bucketNumber);
    }
}
