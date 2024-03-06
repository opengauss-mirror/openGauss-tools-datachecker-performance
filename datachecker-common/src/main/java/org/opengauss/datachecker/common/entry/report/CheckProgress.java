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

package org.opengauss.datachecker.common.entry.report;

import com.alibaba.fastjson.annotation.JSONType;
import lombok.Data;
import lombok.experimental.Accessors;
import org.opengauss.datachecker.common.entry.enums.CheckMode;

import java.time.LocalDateTime;

/**
 * @author ：wangchao
 * @date ：Created in 2023/2/24
 * @since ：11
 */
@Data
@Accessors(chain = true)
@JSONType(orders = {"mode", "status", "tableCount", "completeCount", "total", "rows", "avgSpeed", "cost", "startTime",
    "currentTime", "endTime"})
public class CheckProgress {
    private short status;
    private int tableCount;
    private int completeCount;
    private long total;
    private long rows;
    private int avgSpeed;
    private long cost;
    private CheckMode mode;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private LocalDateTime currentTime;

    public String toSimpleString() {
        return "[mode:" + mode + " , tableCount:" + tableCount + " , completeCount:" + completeCount + " , total:"
            + total + " , rows:" + rows + " , avgSpeed:" + avgSpeed + " , cost:" + cost + "s]";
    }
}
