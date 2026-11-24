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

import java.util.List;

/**
 * DebeziumData
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/24
 * @since ：11
 */
@Data
public class DebeziumData {
    private DebeziumSchema schema;
    private DebeziumPayload payload;
}

@Data
class DebeziumSchema {
    private String name;
    private String type;
    private List<Field> fields;
    private boolean optional;
}

@Data
class Field {
    private String name;
    private String field;
    private String type;
    private List<Field> fields;
    private boolean optional;
}