/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
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

package org.opengauss.datachecker.common.entry.common;

import lombok.Data;

/**
 * PointPair
 *
 * @author ：wangchao
 * @date ：Created in 2023/11/16
 * @since ：11
 */
@Data
public class PointPair {
    private Object checkPoint;

    /**
     * current checkpoint of table ,contains how manny rows
     */
    private long rowCount;
    private int sliceIdx;

    public PointPair(Object checkPoint, long rowCount) {
        this(checkPoint, rowCount, 0);
    }

    public PointPair(Object checkPoint, long rowCount, int sliceIdx) {
        this.checkPoint = checkPoint;
        this.rowCount = rowCount;
        this.sliceIdx = sliceIdx;
    }

    @Override
    public String toString() {
        return "[ " + checkPoint + " , " + rowCount + " idx: " + sliceIdx + " ]";
    }
}
