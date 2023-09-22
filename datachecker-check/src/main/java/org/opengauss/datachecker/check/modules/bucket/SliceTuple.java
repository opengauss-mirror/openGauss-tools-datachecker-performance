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

import lombok.Getter;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;

import java.util.List;

/**
 * CheckTuple
 *
 * @author ：wangchao
 * @date ：Created in 2023/6/24
 * @since ：11
 */
@Getter
public class SliceTuple {
    private Endpoint endpoint;
    private SliceExtend slice;
    private List<Bucket> buckets;

    private SliceTuple(Endpoint endpoint, SliceExtend slice, List<Bucket> buckets) {
        this.endpoint = endpoint;
        this.slice = slice;
        this.buckets = buckets;
    }

    /**
     * sliceTuple create,when slice data merkle tree building
     *
     * @param endpoint endpoint
     * @param slice    slice
     * @param buckets  buckets
     * @return sliceTuple
     */
    public static SliceTuple of(Endpoint endpoint, SliceExtend slice, List<Bucket> buckets) {
        return new SliceTuple(endpoint, slice, buckets);
    }

    /**
     * slice bucket size
     *
     * @return bucket size
     */
    public int getBucketSize() {
        return buckets.size();
    }
}
