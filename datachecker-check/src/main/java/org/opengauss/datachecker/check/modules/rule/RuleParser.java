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

package org.opengauss.datachecker.check.modules.rule;

import org.apache.commons.collections4.CollectionUtils;
import org.opengauss.datachecker.check.config.RuleConfig;
import org.opengauss.datachecker.common.entry.common.Rule;
import org.opengauss.datachecker.common.entry.enums.RuleType;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * RuleParser
 *
 * @author ：wangchao
 * @date ：Created in 2022/11/30
 * @since ：11
 */
public class RuleParser {
    /**
     * Filter Rule Configuration Resolution
     *
     * @param config Filter Rule Configuration
     * @return Filter Rule
     */
    public Map<RuleType, List<Rule>> parser(RuleConfig config) {
        Assert.notNull(config, "the rule config cannot be empty");
        if (config.isEnable()) {
            Map<RuleType, List<Rule>> rules = new HashMap<>();
            parseTableRules(rules, config.getTable());
            parseRowRules(rules, config.getRow());
            parseColumnRules(rules, config.getColumn());
            return rules;
        }
        return new HashMap<>();
    }

    private void parseTableRules(Map<RuleType, List<Rule>> rules, List<Rule> tableRuleConfig) {
        putRules(rules, RuleType.TABLE, tableRuleConfig);
    }

    private void parseRowRules(Map<RuleType, List<Rule>> rules, List<Rule> rowRuleConfig) {
        putRules(rules, RuleType.ROW, rowRuleConfig);
    }

    private void parseColumnRules(Map<RuleType, List<Rule>> rules, List<Rule> columnRuleConfig) {
        putRules(rules, RuleType.COLUMN, columnRuleConfig);
    }

    private void putRules(Map<RuleType, List<Rule>> rules, RuleType ruleType, List<Rule> ruleList) {
        if (CollectionUtils.isNotEmpty(ruleList)) {
            rules.put(ruleType, ruleList);
        }
    }
}
