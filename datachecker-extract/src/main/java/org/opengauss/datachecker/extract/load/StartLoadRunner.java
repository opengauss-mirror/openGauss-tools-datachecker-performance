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

package org.opengauss.datachecker.extract.load;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.service.MemoryManagerService;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.SpringUtil;
import org.opengauss.datachecker.extract.config.DataSourceConfig;
import org.opengauss.datachecker.extract.config.DruidDataSourceConfig;
import org.opengauss.datachecker.extract.resource.ResourceManager;
import org.opengauss.datachecker.extract.service.ConfigManagement;
import org.opengauss.datachecker.extract.slice.SliceProcessorContext;
import org.opengauss.datachecker.extract.task.ExtractThreadSupport;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * StartLoadRunner
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/17
 * @since ：11
 */
@Component
public class StartLoadRunner implements ApplicationRunner {
    private static final Logger log = LogUtils.getLogger();
    @Resource
    private ConfigManagement configManagement;
    @Resource
    private ResourceManager resourceManager;
    @Resource
    private MemoryManagerService memoryManagerService;
    @Resource
    private SliceProcessorContext sliceProcessorContext;


    @Override
    public void run(ApplicationArguments args) {
        // if extract boot start finished,then running.
        configManagement.loadExtractProperties();
        memoryManagerService.startMemoryManager(ConfigCache.getBooleanValue(ConfigConstants.MEMORY_MONITOR));
        resourceManager.initMaxConnectionCount();

        initExtractContextDataSource();
        sliceProcessorContext.startSliceStatusFeedbackService();
    }

    private void initExtractContextDataSource() {
        DataSourceConfig config = SpringUtil.getBean(DataSourceConfig.class);
        ExtractThreadSupport context = SpringUtil.getBean("extractThreadSupport",ExtractThreadSupport.class);
        if (config instanceof DruidDataSourceConfig) {
            context.setDataSource(((DruidDataSourceConfig) config).druidDataSource());
        }
    }
}
