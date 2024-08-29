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
import lombok.experimental.Accessors;
import org.opengauss.datachecker.common.constant.Constants;

import java.util.List;

/**
 * Source side data change log
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/14
 * @since ：11
 */
@Schema(description = "Source side data change log")
@Data
@Accessors(chain = true)
public class SourceDataLog {
    private static final String PRIMARY_DELIMITER = Constants.PRIMARY_DELIMITER;
    /**
     * Data change log corresponding table name
     */
    @Schema(name = "tableName")
    private String tableName;
    private long beginOffset = -1;
    /**
     * List of primary key field names of the current table
     */
    @Schema(name = "compositePrimarys", description = "List of primary key field names of the current table")
    private List<String> compositePrimarys;

    /**
     * <pre>
     * List of primary key values of data changes of the same data operation type {@code operateCategory} <p>
     * Single primary key table: primary key values are directly added to the {@code compositePrimaryValues} set< p>
     * Composite primary key: assemble the primary key values and splice them according to
     * the order of the primary key fields recorded in {@code compositePrimarys}. Linker {@value PRIMARY_DELIMITER}
     * </pre>
     */
    @Schema(name = "compositePrimaryValues")
    private List<String> compositePrimaryValues;

    /**
     * source data of compositePrimarys size
     *
     * @return length
     */
    public int length() {
        return compositePrimarys.size();
    }
}
