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

package org.opengauss.datachecker.extract.data.access;

import org.opengauss.datachecker.common.entry.common.DataAccessParam;
import org.opengauss.datachecker.common.entry.enums.OgCompatibility;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.PrimaryColumnBean;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.data.mapper.OpgsMetaDataMapper;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Objects;

/**
 * OpgsDataAccessService
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/10
 * @since ：11
 */
public class OpgsDataAccessService extends AbstractDataAccessService {
    private OpgsMetaDataMapper opgsMetaDataMapper;

    public OpgsDataAccessService(OpgsMetaDataMapper opgsMetaDataMapper) {
        this.opgsMetaDataMapper = opgsMetaDataMapper;
    }

    @Override
    @PostConstruct
    public boolean isOgCompatibilityB() {
        isOgCompatibilityB = Objects.equals(OgCompatibility.B, opgsMetaDataMapper.sqlCompatibility());
        return false;
    }

    @Override
    public boolean health() {
        return opgsMetaDataMapper.health();
    }

    @Override
    public List<String> queryTableNameList() {
        return opgsMetaDataMapper.queryTableNameList(properties.getSchema());
    }

    @Override
    public List<PrimaryColumnBean> queryTablePrimaryColumns() {
        return opgsMetaDataMapper.queryTablePrimaryColumns(properties.getSchema());
    }

    @Override
    public List<PrimaryColumnBean> queryTablePrimaryColumns(String tableName) {
        return opgsMetaDataMapper.queryTablePrimaryColumnsByTableName(properties.getSchema(),tableName);
    }

    @Override
    public List<ColumnsMetaData> queryTableColumnsMetaData(String tableName) {
        return opgsMetaDataMapper.queryTableColumnsMetaData(properties.getSchema(), tableName);
    }

    @Override
    public TableMetadata queryTableMetadata(String tableName) {
        return wrapperTableMetadata(opgsMetaDataMapper.queryTableMetadata(properties.getSchema(), tableName));
    }

    @Override
    public List<TableMetadata> queryTableMetadataList() {
        return wrapperTableMetadata(opgsMetaDataMapper.queryTableMetadataList(properties.getSchema()));
    }

    @Override
    public long rowCount(String tableName) {
        return opgsMetaDataMapper.rowCount(properties.getSchema(), tableName);
    }

    @Override
    public String min(DataAccessParam param) {
        return opgsMetaDataMapper.min(param);
    }

    @Override
    public String max(DataAccessParam param) {
        return opgsMetaDataMapper.max(param);
    }

    @Override
    public String next(DataAccessParam param) {
        return opgsMetaDataMapper.next(param);
    }

    @Override
    public List<Object> queryPointList(DataAccessParam param) {
        return opgsMetaDataMapper.queryPointList(param);
    }
}
