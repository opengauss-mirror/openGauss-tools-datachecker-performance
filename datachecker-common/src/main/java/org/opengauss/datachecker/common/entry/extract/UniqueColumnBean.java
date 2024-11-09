/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
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

/**
 * UniqueColumnBean
 *
 * @author ：wangchao
 * @date ：Created in 2023/12/23
 * @since ：11
 */
@Data
public class UniqueColumnBean {
    /**
     * Table
     */
    private String tableName;

    /**
     * Primary key column name
     */
    private String columnName;

    /**
     * Index identifier
     */
    private String indexIdentifier;

    /**
     * Column index
     */
    private Integer colIdx;
}