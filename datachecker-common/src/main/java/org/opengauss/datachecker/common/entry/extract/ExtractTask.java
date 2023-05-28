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

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

/**
 * ExtractTask
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/1
 * @since ：11
 */
@ToString
@Data
@Accessors(chain = true)
public class ExtractTask {
    /**
     * taskName
     */
    private String taskName;

    /**
     * tableName
     */
    private String tableName;

    /**
     * Total number of tasks split: 1 means not split,
     * and greater than 1 means divided into divisionsTotalNumber tasks
     */
    private int divisionsTotalNumber = 1;

    /**
     * Current table, split task sequence
     */
    private int divisionsOrdinal = 1;

    /**
     * Start position of task execution
     */
    private long start = 0L;

    /**
     * Task execution offset
     */
    private long offset;
    private Topic topic;
    /**
     * Table metadata information
     */
    private TableMetadata tableMetadata;

    /**
     * Whether to slice the table corresponding to the current task
     *
     * @return If true is returned, it indicates fragmentation, and false indicates no fragmentation
     */
    public boolean isDivisions() {
        return divisionsTotalNumber > 1;
    }
}
