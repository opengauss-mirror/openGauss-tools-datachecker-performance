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

import lombok.Getter;
import org.opengauss.datachecker.common.service.DynamicThreadPoolManager;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.kafka.KafkaAdminService;
import org.opengauss.datachecker.extract.resource.ResourceManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.sql.DataSource;

/**
 * ExtractThreadSupport
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/30
 * @since ：11
 */
@Getter
@Service
public class ExtractThreadSupport {
    @Resource
    private DataSource dataSource;
    @Resource
    private ResourceManager resourceManager;
    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private KafkaAdminService kafkaAdminService;
    @Resource
    private CheckingFeignClient checkingFeignClient;
    @Resource
    private ExtractProperties extractProperties;
    @Resource
    private DynamicThreadPoolManager dynamicThreadPoolManager;
}
