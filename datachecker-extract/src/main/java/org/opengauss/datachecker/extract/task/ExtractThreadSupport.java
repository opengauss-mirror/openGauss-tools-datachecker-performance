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

package org.opengauss.datachecker.extract.task;

import org.opengauss.datachecker.common.entry.common.ExtractContext;
import org.opengauss.datachecker.common.service.DynamicThreadPoolManager;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.data.access.DataAccessService;
import org.opengauss.datachecker.extract.resource.ResourceManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Resource;
import javax.sql.DataSource;

/**
 * ExtractThreadSupport
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/30
 * @since ：11
 */
@Service
public class ExtractThreadSupport {
    private static final int DEFAULT_MAXIMUM_TABLE_SLICE_SIZE = 10000;
    @Value("${spring.check.maximum-table-slice-size}")
    private int maximumTableSliceSize = DEFAULT_MAXIMUM_TABLE_SLICE_SIZE;
    private DataSource dataSource;
    @Resource
    private DataAccessService dataAccessService;
    @Resource
    private ResourceManager resourceManager;
    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;
    @Resource
    private CheckingFeignClient checkingFeignClient;
    @Resource
    private ExtractProperties extractProperties;
    @Resource
    private DynamicThreadPoolManager dynamicThreadPoolManager;
    private ExtractContext context;

    @PostConstruct
    public void initContext() {
        context = new ExtractContext();
        context.setSchema(extractProperties.getSchema());
        context.setEndpoint(extractProperties.getEndpoint());
        context.setDatabaseType(extractProperties.getDatabaseType());
        context.setMaximumTableSliceSize(getMaximumTableSliceSize());
    }

    private int getMaximumTableSliceSize() {
        if (maximumTableSliceSize == 0) {
            return maximumTableSliceSize;
        } else if (maximumTableSliceSize >= DEFAULT_MAXIMUM_TABLE_SLICE_SIZE) {
            return maximumTableSliceSize;
        } else {
            return DEFAULT_MAXIMUM_TABLE_SLICE_SIZE;
        }
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public ResourceManager getResourceManager() {
        return resourceManager;
    }

    public KafkaTemplate<String, String> getKafkaTemplate() {
        return kafkaTemplate;
    }

    public CheckingFeignClient getCheckingFeignClient() {
        return checkingFeignClient;
    }

    public DynamicThreadPoolManager getDynamicThreadPoolManager() {
        return dynamicThreadPoolManager;
    }

    public ExtractContext getContext() {
        return context;
    }

    public DataAccessService getDataAccessService() {
        return dataAccessService;
    }

    @PreDestroy
    public void destroy() {
        kafkaTemplate.destroy();
    }
}
