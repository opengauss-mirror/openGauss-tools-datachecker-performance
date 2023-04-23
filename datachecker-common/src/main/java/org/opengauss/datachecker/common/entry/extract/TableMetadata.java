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

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * Table metadata information
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/14
 * @since ：11
 */
@Schema(name = "Table metadata information")
@Data
@Accessors(chain = true)
@ToString
public class TableMetadata {
    /**
     * tableName
     */
    private String tableName;

    /**
     * Total table data
     */
    private long tableRows;

    private long avgRowLength;

    /**
     * Primary key column properties
     */
    private List<ColumnsMetaData> primaryMetas;

    /**
     * Table column properties
     */
    private List<ColumnsMetaData> columnsMetas;

    private ConditionLimit conditionLimit;
}
