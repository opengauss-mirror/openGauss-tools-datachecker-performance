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

/**
 * PayloadSource
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/30
 * @since ：11
 */
@Data
public class PayloadSource {
    private String version;
    private String connector;
    private String name;
    private long ts_ms;
    private boolean snapshot;
    private String db;
    private String sequence;
    private String schema;
    private String table;
    private String txId;
    private String scn;
    private String commit_scn;
    private String lcr_position;
}