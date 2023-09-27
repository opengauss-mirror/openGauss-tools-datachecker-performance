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
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.extract.data.access.CsvDataAccessService;
import org.opengauss.datachecker.extract.data.access.DataAccessService;
import org.opengauss.datachecker.extract.data.access.MysqlDataAccessService;
import org.opengauss.datachecker.extract.data.access.OpgsDataAccessService;
import org.opengauss.datachecker.extract.data.mapper.MetaDataMapper;
import org.opengauss.datachecker.extract.data.mapper.MysqlMetaDataMapper;
import org.opengauss.datachecker.extract.data.mapper.OpgsMetaDataMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Objects;

/**
 * CSV DataSourceConfig
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Configuration
public class DataAccessAutoConfiguration {
    @Value("${spring.extract.endpoint}")
    private Endpoint endpoint;
    @Value("${spring.extract.databaseType}")
    private DataBaseType databaseType;
    @Value("${spring.extract.dataLoadMode}")
    private DataLoad dataLoadMode;

    @Resource
    private MysqlMetaDataMapper mysqlMetaDataMapper;
    @Resource
    private OpgsMetaDataMapper opgsMetaDataMapper;

    @Bean
    public DataAccessService createDataAccessService() {
        Map<DataBaseType, MetaDataMapper> mybatisMappers =
            Map.of(DataBaseType.MS, this.mysqlMetaDataMapper, DataBaseType.OG, opgsMetaDataMapper);
        return new DataAccessServiceBeanFactory(mybatisMappers).create(endpoint, databaseType, dataLoadMode);
    }

    static class DataAccessServiceBeanFactory {
        private final Map<DataBaseType, MetaDataMapper> mybatisMappers;

        public DataAccessServiceBeanFactory(Map<DataBaseType, MetaDataMapper> mybatisMappers) {
            this.mybatisMappers = mybatisMappers;
        }

        public DataAccessService create(Endpoint endpoint, DataBaseType dataBaseType, DataLoad dataLoad) {
            if (Objects.equals(DataLoad.CSV, dataLoad)) {
                return createDataLoadOfCsv(endpoint, dataBaseType);
            } else {
                return createOf(dataBaseType);
            }
        }

        private DataAccessService createDataLoadOfCsv(Endpoint endpoint, DataBaseType dataBaseType) {
            if (Objects.equals(Endpoint.SOURCE, endpoint)) {
                return new CsvDataAccessService();
            } else {
                return createOf(dataBaseType);
            }
        }

        private DataAccessService createOf(DataBaseType targetDataBaseType) {
            if (Objects.equals(DataBaseType.OG, targetDataBaseType)) {
                return new OpgsDataAccessService((OpgsMetaDataMapper) mybatisMappers.get(targetDataBaseType));
            } else if (Objects.equals(DataBaseType.MS, targetDataBaseType)) {
                return new MysqlDataAccessService((MysqlMetaDataMapper) mybatisMappers.get(targetDataBaseType));
            } else {
                return new MysqlDataAccessService((MysqlMetaDataMapper) mybatisMappers.get(DataBaseType.MS));
            }
        }
    }
}
