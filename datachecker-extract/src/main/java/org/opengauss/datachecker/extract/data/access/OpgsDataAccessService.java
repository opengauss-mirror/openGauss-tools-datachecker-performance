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
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.config.DruidDataSourceConfig;
import org.opengauss.datachecker.extract.data.mapper.OpgsMetaDataMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * OpgsDataAccessService
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/10
 * @since ：11
 */
@Slf4j
@Service
@ConditionalOnBean(DruidDataSourceConfig.class)
@ConditionalOnProperty(prefix = "spring.extract", name = "databaseType", havingValue = "OG")
public class OpgsDataAccessService extends AbstractDataAccessService {
    @Resource
    private OpgsMetaDataMapper opgsMetaDataMapper;

    @Override
    public boolean health() {
        return opgsMetaDataMapper.health();
    }

    @Override
    public List<String> queryTableNameList() {
        return opgsMetaDataMapper.queryTableNameList(properties.getSchema());
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
}
