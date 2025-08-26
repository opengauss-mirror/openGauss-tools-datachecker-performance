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

package org.opengauss.datachecker.common.entry.check;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * Debezium incremental migration verification initialization configuration
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/24
 * @since ：11
 */
@Schema(name = "Debezium incremental migration verification initialization configuration")
@Data
@Accessors(chain = true)
public class IncrementCheckConfig {
    /**
     * Debezium incremental migration topic, debezium monitors table incremental data,
     * and uses a single topic for incremental data management
     */
    @Schema(name = "debeziumTopic", required = true)
    @NotNull(message = "Debezium incremental migration topic cannot be empty")
    private String debeziumTopic;

    @Schema(name = "groupId", description = "Topic grouping")
    @NotNull(message = "Debezium incremental migration topic groupid cannot be empty")
    private String groupId;

    @Schema(name = "partitions", description = "Topic partition", defaultValue = "1")
    private int partitions = 1;

    /**
     * Incremental migration table name list
     */
    @Schema(name = "debeziumTables", required = true, description = "Incremental migration table name list")
    @NotEmpty(message = "Incremental migration table name list cannot be empty")
    private List<String> debeziumTables;
}
