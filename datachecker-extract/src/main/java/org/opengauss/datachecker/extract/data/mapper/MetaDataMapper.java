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

package org.opengauss.datachecker.extract.data.mapper;

import org.apache.ibatis.annotations.Param;
import org.opengauss.datachecker.common.entry.common.DataAccessParam;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;

import java.util.List;

/**
 * Mybatis mapper, base operate MetaDataMapper
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/11
 * @since ：11
 */
public interface MetaDataMapper {
    /**
     * check jdbc health
     *
     * @return true | false
     */
    boolean health();

    /**
     * query schema table list
     *
     * @param schema schema
     * @return table name list
     */
    List<String> queryTableNameList(String schema);

    /**
     * query TableMetadata list
     *
     * @param schema schema
     * @return TableMetadata list
     */
    List<TableMetadata> queryTableMetadataList(String schema);

    /**
     * query table ColumnsMetaData
     *
     * @param schema    schema
     * @param tableName tableName
     * @return ColumnsMetaData
     */
    List<ColumnsMetaData> queryTableColumnsMetaData(@Param("schema") String schema, @Param("name") String tableName);

    /**
     * query table TableMetadata
     *
     * @param schema    schema
     * @param tableName tableName
     * @return TableMetadata
     */
    TableMetadata queryTableMetadata(@Param("schema") String schema, @Param("name") String tableName);

    /**
     * query table row count
     *
     * @param schema    schema
     * @param tableName tableName
     * @return row count
     */
    long rowCount(@Param("schema") String schema, @Param("name") String tableName);

    /**
     * query table min value
     *
     * @param param param
     * @return val
     */
    String min(@Param("param") DataAccessParam param);

    /**
     * query table max value
     *
     * @param param param
     * @return val
     */
    String max(@Param("param") DataAccessParam param);

    /**
     * query table next value
     *
     * @param param param
     * @return val
     */
    String next(@Param("param") DataAccessParam param);
}
