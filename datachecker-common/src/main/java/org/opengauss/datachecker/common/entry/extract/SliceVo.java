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

package org.opengauss.datachecker.common.entry.extract;

import com.alibaba.fastjson.annotation.JSONType;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * SliceVo
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/18
 * @since ：11
 */
@EqualsAndHashCode(callSuper = true)
@Data
@JSONType(orders = {"type", "schema", "table", "name", "no", "total", "beginIdx", "endIdx", "fetchSize"})
public class SliceVo extends BaseSlice {
    /**
     * slice data extract status
     * register status is zero
     * source update slice status 1
     * sink update slice status 2
     */
    private int status = 0;
    private boolean isExistTableRows;

    /**
     * table metadata hash value
     */
    private long tableHash = -1;

    /**
     * slice data send topic partition no
     */
    private int ptn = 0;

    /**
     * kafka topic partitions count
     */
    private int ptnNum = 1;

    /**
     * slice data extract status
     * register status is zero
     * source update slice status 1
     * sink update slice status 2
     * if source and sink all updated,status value is 3
     * if slice is checked, update slice status 4, then slice status value is 7
     *
     * @param update update
     */
    public void setStatus(int update) {
        this.status = this.status | update;
    }

    @Override
    public String toString() {
        return super.toString() + "status=" + status + ", tableHash=" + tableHash + ", ptn=" + ptn + ", ptnNum="
            + ptnNum;
    }

    public String toSimpleString() {
        if (super.getTotal() == 1) {
            return super.getName() + " total=" + super.getTotal() + " no=" + super.getNo() + ", [ fetch full ]";
        }
        if (super.getInIds() != null && !super.getInIds().isEmpty()) {
            return super.getName() + " total=" + super.getTotal() + " no=" + super.getNo() + ", " + super.getInIds();
        }
        return super.getName() + " total=" + super.getTotal() + " no=" + super.getNo() + ", [" + super.getBeginIdx()
            + " , " + super.getEndIdx() + " ]";
    }
}
