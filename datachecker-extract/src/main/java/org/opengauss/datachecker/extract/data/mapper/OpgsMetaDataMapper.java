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

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.opengauss.datachecker.common.entry.enums.OgCompatibility;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;

import java.util.List;

/**
 * OpgsMetaDataMapper
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/11
 * @since ：11
 */
@Mapper
public interface OpgsMetaDataMapper extends MetaDataMapper {

    /**
     * check og database compatibility
     *
     * @return compatibility
     */
    @Select("show sql_compatibility;")
    OgCompatibility sqlCompatibility();

    /**
     * query table ColumnsMetaData
     *
     * @param schema    schema
     * @param tableName tableName
     * @return ColumnsMetaData
     */
    List<ColumnsMetaData> queryTableColumnsMetaDataB(@Param("schema") String schema, @Param("name") String tableName);
}
