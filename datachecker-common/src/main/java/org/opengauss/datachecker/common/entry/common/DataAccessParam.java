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

package org.opengauss.datachecker.common.entry.common;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * DataAccessParam
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/27
 * @since ：11
 */
@Accessors(chain = true)
@Data
public class DataAccessParam {
    private String schema;
    private String name;
    private String colName;
    private String preValue;
    private long offset;
}
