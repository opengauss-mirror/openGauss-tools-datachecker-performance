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

package org.opengauss.datachecker.extract.debezium;

import lombok.ToString;

import java.util.Map;

/**
 * DebeziumDataBean
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/1
 * @since ：11
 */
@ToString
public class DebeziumDataBean {
    private String table;
    private long offset;
    private Map<String, String> data;

    public DebeziumDataBean(String table, long offset, Map<String, String> data) {
        this.table = table;
        this.offset = offset;
        this.data = data;
    }

    public String getTable() {
        return table;
    }

    public long getOffset() {
        return offset;
    }

    public Map<String, String> getData() {
        return data;
    }
}
