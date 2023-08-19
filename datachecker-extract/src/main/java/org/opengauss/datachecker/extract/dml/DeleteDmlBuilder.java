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

import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.extract.constants.ExtConstants;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.stream.IntStream;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/13
 * @since ：11
 */
public class DeleteDmlBuilder extends DmlBuilder {
    public DeleteDmlBuilder(DataBaseType databaseType, boolean ogCompatibility) {
        super(databaseType, ogCompatibility);
    }
    /**
     * build Schema
     *
     * @param schema Schema
     * @return DeleteDMLBuilder
     */
    public DeleteDmlBuilder schema(@NotNull String schema) {
        super.buildSchema(schema);
        return this;
    }

    /**
     * build tableName
     *
     * @param tableName tableName
     * @return DeleteDMLBuilder
     */
    public DeleteDmlBuilder tableName(@NotNull String tableName) {
        super.buildTableName(tableName);
        return this;
    }

    /**
     * Generate a single primary key field delete from schema.table where pk = Parameter+conditional statement
     *
     * @param primaryMeta Primary key metadata
     * @return DeleteDMLBuilder
     */
    public DeleteDmlBuilder condition(@NonNull ColumnsMetaData primaryMeta, String value) {
        Assert.isTrue(StringUtils.isNotEmpty(primaryMeta.getColumnName()),
            "Table metadata primary key field name is empty");
        if (DIGITAL.contains(primaryMeta.getDataType())) {
            condition = primaryMeta.getColumnName().concat(EQUAL).concat(value);
        } else {
            condition =
                primaryMeta.getColumnName().concat(EQUAL).concat(SINGLE_QUOTES).concat(value).concat(SINGLE_QUOTES);
        }
        return this;
    }

    /**
     * Construct conditional delete statements for composite primary key parameters<p>
     * delete from schema.table where pk1 = pk1_val and pk2 = pk2_val<p>
     *
     * @param compositeKey composite primary key
     * @param primaryMetas Primary key metadata
     * @return SelectDMLBuilder
     */
    public DeleteDmlBuilder conditionCompositePrimary(String compositeKey, List<ColumnsMetaData> primaryMetas) {
        condition = buildCondition(compositeKey, primaryMetas);
        return this;
    }

    /**
     * Build primary key filter (where) conditions <p>
     * pk = pk_value <p>
     * or <p>
     * pk = 'pk_value' <p>
     *
     * @param compositeKey composite primary key
     * @param primaryMetas Primary key metadata
     * @return Return the primary key where condition
     */
    public String buildCondition(String compositeKey, List<ColumnsMetaData> primaryMetas) {
        final String[] split = compositeKey.split(ExtConstants.PRIMARY_DELIMITER);
        StringBuffer conditionBuffer = new StringBuffer();
        if (split.length == primaryMetas.size()) {
            IntStream.range(0, primaryMetas.size()).forEach(idx -> {
                String condition = "";
                final ColumnsMetaData mate = primaryMetas.get(idx);
                if (idx > 0) {
                    condition = condition.concat(AND);
                }
                if (DIGITAL.contains(mate.getDataType())) {
                    condition = condition.concat(mate.getColumnName()).concat(EQUAL).concat(split[idx]);
                } else {
                    condition =
                        condition.concat(mate.getColumnName()).concat(EQUAL).concat(SINGLE_QUOTES).concat(split[idx])
                                 .concat(SINGLE_QUOTES);
                }
                conditionBuffer.append(condition);
            });
        }
        return conditionBuffer.toString();
    }

    public String build() {
        StringBuffer sb = new StringBuffer();
        sb.append(Fragment.DELETE).append(Fragment.FROM).append(schema).append(Fragment.LINKER).append(tableName)
          .append(Fragment.WHERE).append(condition).append(Fragment.END);
        return sb.toString();
    }
}
