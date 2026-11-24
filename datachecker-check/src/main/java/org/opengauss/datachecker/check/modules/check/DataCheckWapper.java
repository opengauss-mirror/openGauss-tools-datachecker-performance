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

package org.opengauss.datachecker.check.modules.check;

import org.opengauss.datachecker.check.modules.bucket.Bucket;
import org.opengauss.datachecker.check.modules.bucket.BuilderBucketHandler;
import org.opengauss.datachecker.common.entry.check.Pair;
import org.springframework.lang.NonNull;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/18
 * @since ：11
 */
public class DataCheckWapper {

    /**
     * Align the bucket list data according to the statistical results of source and destination bucket
     * difference information {@code bucketNumberDiffMap}.
     *
     * @param bucketNumberDiffMap Source destination bucket difference information
     * @param sourceBucketList    Source bucket list
     * @param sinkBucketList      Sink bucket list
     */
    public void alignAllBuckets(Map<Integer, Pair<Integer, Integer>> bucketNumberDiffMap,
        @NonNull List<Bucket> sourceBucketList, @NonNull List<Bucket> sinkBucketList) {
        if (!CollectionUtils.isEmpty(bucketNumberDiffMap)) {
            bucketNumberDiffMap.forEach((number, pair) -> {
                if (pair.getSource() == -1) {
                    sourceBucketList.add(BuilderBucketHandler.builderEmpty(number));
                }
                if (pair.getSink() == -1) {
                    sinkBucketList.add(BuilderBucketHandler.builderEmpty(number));
                }
            });
        }
    }
}
