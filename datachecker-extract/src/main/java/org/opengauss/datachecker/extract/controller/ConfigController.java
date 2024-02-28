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

package org.opengauss.datachecker.extract.controller;

import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.common.GlobalConfig;
import org.opengauss.datachecker.common.entry.csv.CsvPathConfig;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.service.ProcessLogService;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.web.Result;
import org.opengauss.datachecker.extract.data.BaseDataService;
import org.opengauss.datachecker.extract.load.ExtractEnvironmentContext;
import org.opengauss.datachecker.extract.service.ConfigManagement;
import org.opengauss.datachecker.extract.service.RuleAdapterService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Objects;

/**
 * config Controller
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/23
 * @since ：11
 */
@RestController
public class ConfigController {
    private static final Logger log = LogUtils.getLogger();

    @Resource
    private ExtractEnvironmentContext context;
    @Resource
    private BaseDataService baseDataService;
    @Resource
    private RuleAdapterService ruleAdapterService;
    @Resource
    private ConfigManagement configManagement;
    @Resource
    private ProcessLogService processLogService;
    /**
     * Distribution Extraction config
     *
     * @param config config
     */
    @PostMapping("/extract/config/distribute")
    public void distributeConfig(@RequestBody GlobalConfig config) {
        CheckMode checkMode = config.getCheckMode();
        ConfigCache.setCheckMode(checkMode);
        ConfigCache.put(ConfigConstants.CHECK_PATH, config.getProcessPath());
        Map<String, Object> commonConfig = config.getCommonConfig();
        if (MapUtils.isNotEmpty(commonConfig)) {
            commonConfig.forEach(ConfigCache::put);
        }
        baseDataService.initDataSourceSqlMode2ConfigCache();
        processLogService.saveProcessLog();
        ruleAdapterService.init(config.getRules());
        log.info("init filter rule config ");
        if (Objects.equals(checkMode, CheckMode.FULL) || Objects.equals(checkMode, CheckMode.INCREMENT)) {
            context.loadDatabaseMetaData();
        }
    }

    /**
     * Turn on verification
     *
     * @return verification process info
     */
    @PostMapping("/csv/config/distribute")
    public Result<Void> distributeConfig(@RequestBody CsvPathConfig csvPathConfig) {
        configManagement.initCsvConfig(csvPathConfig);
        log.info("init csv config ");
        return Result.success();
    }
}
