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

package org.opengauss.datachecker.check.annotation.aspect;

import com.alibaba.fastjson.annotation.JSONType;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * StatisticalRecord
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/20
 * @since ：11
 */
@Data
@Accessors(chain = true)
@JSONType(orders = {"name", "start", "end", "cost"})
public class StatisticalRecord {
    private String start;
    private String end;
    private long cost;
    private String name;
}