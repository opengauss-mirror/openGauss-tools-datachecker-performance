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

package org.opengauss.datachecker.extract.config;

import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.DataLoad;
import org.opengauss.datachecker.extract.data.mapper.MetaDataMapper;
import org.opengauss.datachecker.extract.data.mapper.MysqlMetaDataMapper;
import org.opengauss.datachecker.extract.data.mapper.OpgsMetaDataMapper;
import org.opengauss.datachecker.extract.data.mapper.OracleMetaDataMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

/**
 * CSV DataSourceConfig
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Configuration
@ConditionalOnProperty(prefix = "spring.extract", name = "dataLoadMode", havingValue = "jdbc")
@ConditionalOnBean({DruidDataSourceConfig.class})
public class DataLoadJdbcConfiguration {
    Map<DataBaseType, MetaDataMapper> mybatisMappers = new HashMap<>();
    @Resource
    private MysqlMetaDataMapper mysqlMetaDataMapper;
    @Resource
    private OpgsMetaDataMapper opgsMetaDataMapper;
    @Resource
    private OracleMetaDataMapper oracleMetaDataMapper;

    @Value("${spring.extract.dataLoadMode}")
    private DataLoad dataLoadMode;

    public Map<DataBaseType, MetaDataMapper> createDataMappers() {
        if (DataLoad.JDBC.equals(dataLoadMode)) {
            mybatisMappers.put(DataBaseType.MS, mysqlMetaDataMapper);
            mybatisMappers.put(DataBaseType.OG, opgsMetaDataMapper);
            mybatisMappers.put(DataBaseType.O, oracleMetaDataMapper);
        }
        return mybatisMappers;
    }
}
