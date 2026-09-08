/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
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
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;

import java.util.List;

/**
 * Metadata mapper of the oGRAC source database.
 *
 * @author : xujintao
 * @date : Created in 2026/9/7
 * @since : 11
 */
@Mapper
public interface OgracMetaDataMapper extends MetaDataMapper {
    /**
     * Query the column metadata of all tables under the given schema.
     *
     * @param schema schema name
     * @return column metadata list
     */
    List<ColumnsMetaData> querySchemaColumnsMetaData(@Param("schema") String schema);
}
