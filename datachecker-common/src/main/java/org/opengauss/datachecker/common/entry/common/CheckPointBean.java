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
import lombok.experimental.Accessors;

import org.opengauss.datachecker.common.entry.enums.Endpoint;

/**
 * CheckPointBean
 *
 * @author ：wangchao
 * @date ：Created in 2023/11/16
 * @since ：11
 */
@Data
@Accessors(chain = true)
public class CheckPointBean {
    private Endpoint endpoint;
    private String tableName;
    private String colName;
    private boolean isDigit;

    /**
     * all of the check point list,that count is size
     */
    private int size;
    private PointPair checkPoint;

    @Override
    public String toString() {
        return endpoint + "[" + tableName + "." + colName + "] is digit " + isDigit + ", size=" + size + ", checkPoint="
            + checkPoint;
    }
}
