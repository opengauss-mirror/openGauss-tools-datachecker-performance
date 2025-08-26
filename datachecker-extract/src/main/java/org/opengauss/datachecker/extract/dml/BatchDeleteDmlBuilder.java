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

package org.opengauss.datachecker.extract.dml;

import jakarta.validation.constraints.NotNull;
import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

import java.util.List;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/13
 * @since ：11
 */
public class BatchDeleteDmlBuilder extends DmlBuilder {

    /**
     * build Schema
     *
     * @param schema Schema
     * @return DeleteDMLBuilder
     */
    public BatchDeleteDmlBuilder schema(@NotNull String schema) {
        super.buildSchema(schema);
        return this;
    }

    /**
     * build tableName
     *
     * @param tableName tableName
     * @return DeleteDMLBuilder
     */
    public BatchDeleteDmlBuilder tableName(@NotNull String tableName) {
        super.buildTableName(tableName);
        return this;
    }

    /**
     * Generate a single primary key field[ delete from schema.table where pk in (param...) conditional statement
     *
     * @param primaryMeta Builder primary key metadata
     * @return DeleteDMLBuilder
     */
    public BatchDeleteDmlBuilder conditionPrimary(@NonNull ColumnsMetaData primaryMeta) {
        Assert.isTrue(StringUtils.isNotEmpty(primaryMeta.getColumnName()),
            "Table metadata primary key field name is empty");
        condition = primaryMeta.getColumnName().concat(IN);
        return this;
    }

    /**
     * Construct conditional query statements of composite primary key parameters<p>
     * select columns... from table where (pk1,pk2) in ((pk1_val,pk2_val),(pk1_val,pk2_val))<p>
     *
     * @param primaryMeta
     * @return SelectDMLBuilder
     */
    public BatchDeleteDmlBuilder conditionCompositePrimary(@NonNull List<ColumnsMetaData> primaryMeta) {
        condition = buildConditionCompositePrimary(primaryMeta).concat(IN);
        return this;
    }

    public String build() {
        StringBuffer sb = new StringBuffer();
        sb.append(Fragment.DELETE).append(Fragment.FROM).append(schema).append(Fragment.LINKER).append(tableName)
          .append(Fragment.WHERE).append(condition).append(Fragment.END);
        return sb.toString();
    }
}
