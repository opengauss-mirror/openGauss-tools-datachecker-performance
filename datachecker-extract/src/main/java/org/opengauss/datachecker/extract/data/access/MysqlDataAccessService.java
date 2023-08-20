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

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.common.DataAccessParam;
import org.opengauss.datachecker.common.entry.enums.OgCompatibility;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.config.DruidDataSourceConfig;
import org.opengauss.datachecker.extract.data.mapper.MysqlMetaDataMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Objects;

/**
 * MysqlDataAccessService
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/10
 * @since ：11
 */
@Slf4j
@Service
@ConditionalOnBean(DruidDataSourceConfig.class)
@ConditionalOnProperty(prefix = "spring.extract", name = "databaseType", havingValue = "MS")
public class MysqlDataAccessService extends AbstractDataAccessService {

    @Resource
    private MysqlMetaDataMapper mysqlMetaDataMapper;

    @Override
    public boolean health() {
        return mysqlMetaDataMapper.health();
    }

    @Override
    public boolean isOgCompatibilityB() {
        return false;
    }

    @Override
    public List<String> queryTableNameList() {
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
    public List<TableMetadata> queryTableMetadataList() {
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
}
