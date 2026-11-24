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

package org.opengauss.datachecker.common.entry.debezium;

import lombok.Data;

import java.util.Map;

/**
 * DebeziumPayload
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/30
 * @since ：11
 */
@Data
public class DebeziumPayload {
    private PayloadSource source;
    private Map<String, String> before;
    private Map<String, String> after;
    private String op;
    private String ts_ms;
    private String transaction;
}