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

package org.opengauss.datachecker.check.load;

import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.config.CsvProperties;
import org.opengauss.datachecker.check.config.DataCheckProperties;
import org.opengauss.datachecker.check.config.RuleConfig;
import org.opengauss.datachecker.check.modules.rule.RuleParser;
import org.opengauss.datachecker.check.service.ConfigManagement;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.common.GlobalConfig;
import org.opengauss.datachecker.common.entry.common.Rule;
import org.opengauss.datachecker.common.entry.csv.CsvPathConfig;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.RuleType;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * CheckRuleLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/10/31
 * @since ：11
 */
@Order(98)
@Service
public class CheckConfigDistributeLoader extends AbstractCheckLoader {
    @Resource
    private FeignClientService feignClient;
    @Resource
    private RuleConfig config;
    @Resource
    private CsvProperties csvProperties;
    @Resource
    private DataCheckProperties checkProperties;
    @Resource
    private ConfigManagement configManagement;

    /**
     * Initialize the verification result environment
     */
    @Override
    public void load(CheckEnvironment checkEnvironment) {
        try {
            RuleParser ruleParser = new RuleParser();
            CheckMode checkMode = checkEnvironment.getCheckMode();
            log.info("check service distribute config. {}", checkMode.getDescription());
            if (Objects.equals(CheckMode.INCREMENT, checkMode)) {
                config.tableRuleClear();
                config.rowRuleClear();
            }
            final Map<RuleType, List<Rule>> rules = ruleParser.parser(config);
            GlobalConfig globalConfig = initDistributeGlobalConfig(checkMode, rules);
            feignClient.distributeConfig(checkMode, globalConfig);
            log.info("check distribute rule config success.");
            // distribute csv config
            if (Objects.equals(CheckMode.CSV, checkEnvironment.getCheckMode())) {
                CsvPathConfig csvPathConfig = csvProperties.translate();
                configManagement.setCsvConfig(csvPathConfig);
                feignClient.distributeConfig(csvPathConfig);
                log.info("check distribute csv config success.");
            }
            checkEnvironment.addRules(rules);
            log.info("check service distribute config success.");
        } catch (Exception ignore) {
            log.error("distribute config error: ", ignore);
            throw new CheckingException("distribute config error");
        }
    }

    private GlobalConfig initDistributeGlobalConfig(CheckMode checkMode, Map<RuleType, List<Rule>> rules) {
        GlobalConfig globalConfig = new GlobalConfig();
        globalConfig.setRules(rules);
        globalConfig.setCheckMode(checkMode);
        globalConfig.setProcessPath(checkProperties.getDataPath());
        globalConfig.addCommonConfig(ConfigConstants.FLOATING_POINT_DATA_SUPPLY_ZERO,
            ConfigCache.getBooleanValue(ConfigConstants.FLOATING_POINT_DATA_SUPPLY_ZERO));
        globalConfig.addCommonConfig(ConfigConstants.SQL_MODE_PAD_CHAR_TO_FULL_LENGTH,
            ConfigCache.getBooleanValue(ConfigConstants.SQL_MODE_PAD_CHAR_TO_FULL_LENGTH));
        return globalConfig;
    }
}
