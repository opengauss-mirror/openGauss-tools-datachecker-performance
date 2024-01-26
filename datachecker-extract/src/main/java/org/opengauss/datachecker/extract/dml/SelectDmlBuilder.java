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
import org.opengauss.datachecker.common.util.SqlUtil;
import org.opengauss.datachecker.extract.constants.ExtConstants;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * SelectDmlBuilder
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/13
 * @since ：11
 */
public class SelectDmlBuilder extends DmlBuilder {
    public SelectDmlBuilder(DataBaseType databaseType, boolean ogCompatibility) {super(databaseType, ogCompatibility);}

    /**
     * build SQL column statement fragment
     *
     * @param columnsMetas Field Metadata
     * @return SelectDMLBuilder
     */
    public SelectDmlBuilder columns(@NotNull List<ColumnsMetaData> columnsMetas) {
        super.buildColumns(columnsMetas);
        return this;
    }

    /**
     * build dataBaseType
     *
     * @param dataBaseType dataBaseType
     * @return SelectDMLBuilder
     */
    public SelectDmlBuilder dataBaseType(@NotNull DataBaseType dataBaseType) {
        super.buildDataBaseType(dataBaseType);
        return this;
    }

    /**
     * build Schema
     *
     * @param schema Schema
     * @return SelectDMLBuilder
     */
    public SelectDmlBuilder schema(@NotNull String schema) {
        super.buildSchema(schema);
        return this;
    }

    /**
     * Generate single primary key field SQL: select columns... from where pk in (Parameter...) conditional statement
     *
     * @param primaryMeta Primary key metadata
     * @return SelectDMLBuilder
     */
    public SelectDmlBuilder conditionPrimary(@NonNull ColumnsMetaData primaryMeta) {
        Assert.isTrue(StringUtils.isNotEmpty(primaryMeta.getColumnName()),
            "Table metadata primary key field name is empty");
        condition = SqlUtil.escape(primaryMeta.getColumnName(), dataBaseType, isOgCompatibilityB)
                           .concat(IN);
        return this;
    }

    /**
     * Construct conditional query SQL of composite primary key parameters<p>
     * select columns... from table where (pk1,pk2) in ((pk1_val,pk2_val),(pk1_val,pk2_val))<p>
     *
     * @param primaryMeta Primary key metadata
     * @return SelectDMLBuilder
     */
    public SelectDmlBuilder conditionCompositePrimary(@NonNull List<ColumnsMetaData> primaryMeta) {
        condition = buildConditionCompositePrimary(primaryMeta).concat(IN);
        return this;
    }

    /**
     * Construct the condition of compound primary key parameters to query the value parameter of SQL<p>
     * select columns... from table where (pk1,pk2) in ((pk1_val,pk2_val),(pk1_val,pk2_val))<p>
     *
     * @param primaryMetas  Primary key metadata
     * @param compositeKeys composite Keys value
     * @return SelectDMLBuilder
     */
    public List<Object[]> conditionCompositePrimaryValue(@NonNull List<ColumnsMetaData> primaryMetas,
        List<String> compositeKeys) {
        List<Object[]> batchParam = new ArrayList<>();
        final int size = primaryMetas.size();
        compositeKeys.forEach(compositeKey -> {
            final String[] split = compositeKey.split(ExtConstants.PRIMARY_DELIMITER);
            if (split.length == size) {
                Object[] values = new Object[size];
                IntStream.range(0, primaryMetas.size())
                         .forEach(idx -> {
                             values[idx] = split[idx];
                         });
                batchParam.add(values);
            }
        });
        return batchParam;
    }

    /**
     * build tableName
     *
     * @param tableName tableName
     * @return SelectDMLBuilder
     */
    public SelectDmlBuilder tableName(@NotNull String tableName) {
        super.buildTableName(tableName);
        return this;
    }

    public String build() {
        return Fragment.SELECT + columns + Fragment.FROM + schema + Fragment.LINKER + tableName + Fragment.WHERE
            + condition;
    }
}
