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
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.PrimaryColumnBean;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.data.mapper.MysqlMetaDataMapper;

import java.util.List;

/**
 * MysqlDataAccessService
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/10
 * @since ：11
 */
public class MysqlDataAccessService extends AbstractDataAccessService {
    private MysqlMetaDataMapper mysqlMetaDataMapper;

    public MysqlDataAccessService(MysqlMetaDataMapper mysqlMetaDataMapper) {
        this.mysqlMetaDataMapper = mysqlMetaDataMapper;
    }

    @Override
    public String sqlMode() {
        return mysqlMetaDataMapper.sqlMode();
    }

    @Override
    public boolean health() {
        return mysqlMetaDataMapper.health();
    }

    @Override
    public boolean isOgCompatibilityB() {
        return false;
    }

    @Override
    public List<String> dasQueryTableNameList() {
        return mysqlMetaDataMapper.queryTableNameList(properties.getSchema());
    }

    @Override
    public List<ColumnsMetaData> queryTableColumnsMetaData(String tableName) {
        return mysqlMetaDataMapper.queryTableColumnsMetaData(properties.getSchema(), tableName);
    }

    @Override
    public TableMetadata queryTableMetadata(String tableName) {
        return wrapperTableMetadata(mysqlMetaDataMapper.queryTableMetadata(properties.getSchema(), tableName));
    }

    @Override
    public List<PrimaryColumnBean> queryTablePrimaryColumns() {
        return mysqlMetaDataMapper.queryTablePrimaryColumns(properties.getSchema());
    }

    @Override
    public List<PrimaryColumnBean> queryTablePrimaryColumns(String tableName) {
        return mysqlMetaDataMapper.queryTablePrimaryColumnsByTableName(properties.getSchema(), tableName);
    }

    @Override
    public List<TableMetadata> dasQueryTableMetadataList() {
        return wrapperTableMetadata(mysqlMetaDataMapper.queryTableMetadataList(properties.getSchema()));
    }

    @Override
    public long rowCount(String tableName) {
        return mysqlMetaDataMapper.rowCount(properties.getSchema(), tableName);
    }

    @Override
    public String min(DataAccessParam param) {
        return mysqlMetaDataMapper.min(param);
    }

    @Override
    public String max(DataAccessParam param) {
        return mysqlMetaDataMapper.max(param);
    }

    @Override
    public String next(DataAccessParam param) {
        return mysqlMetaDataMapper.next(param);
    }

    @Override
    public List<Object> queryPointList(DataAccessParam param) {
        return mysqlMetaDataMapper.queryPointList(param);
    }

    @Override
    public boolean dasCheckDatabaseNotEmpty() {
        return mysqlMetaDataMapper.checkDatabaseNotEmpty(properties.getSchema());
    }
}
