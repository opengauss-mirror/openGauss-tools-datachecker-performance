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

import java.time.LocalDateTime;
import java.util.Set;

/**
 * @author ：wangchao
 * @date ：Created in 2023/2/24
 * @since ：11
 */
@Data
@Accessors(chain = true)
@JSONType(
    orders = {"process", "schema", "tableName", "topic", "partition", "beginOffset","rowCount", "diffCount", "cost", "startTime",
        "endTime", "message", "keyInsertSet", "keyUpdateSet", "keyDeleteSet","hasMore"})
public class CheckFailed {
    private String process;
    private String schema;
    private String tableName;
    private String[] topic;
    private long beginOffset;
    private int partition;
    private long rowCount;
    private long diffCount;
    private long cost;
    private String message;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private Set<String> keyInsertSet;
    private Set<String> keyUpdateSet;
    private Set<String> keyDeleteSet;
    private String hasMore;
}
