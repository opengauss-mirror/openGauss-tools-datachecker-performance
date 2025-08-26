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

package org.opengauss.datachecker.check.config.validator;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.annotation.TableRule;
import org.opengauss.datachecker.common.entry.common.Rule;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.util.LogUtils;

import jakarta.validation.ConstraintValidatorContext;
import java.util.List;

import static org.opengauss.datachecker.check.config.validator.RuleConfigValidatorUtil.fetchRuleByPredicate;
import static org.opengauss.datachecker.check.config.validator.RuleConfigValidatorUtil.filterInvalidRegexRule;
import static org.opengauss.datachecker.check.config.validator.RuleConfigValidatorUtil.filterRepeatBy;
import static org.opengauss.datachecker.common.constant.RuleConstants.RULE_BLACK;
import static org.opengauss.datachecker.common.constant.RuleConstants.RULE_WHITE;

/**
 * TableRuleConfigValidator
 * table:
 * - name: white
 *   text: ^[a-zA-Z][a-zA-Z_]+$
 * - name: white
 *   text: ^[a-zA-Z][a-zA-Z0-9_]+$
 * - name: black
 *   text: ^[a-zA-Z][a-zA-Z_]+$
 *
 * @author ：wangchao
 * @date ：Created in 2022/12/7
 * @since ：11
 */
public class RuleConfigTableValidator implements RuleConfigValidator<TableRule> {
    private static final Logger log = LogUtils.getLogger();
    @Override
    public boolean isValid(List<Rule> values, ConstraintValidatorContext context) {
        if (CollectionUtils.isEmpty(values)) {
            return true;
        }
        List<Rule> whiteKeyList = fetchRuleByPredicate(values, rule -> rule.getName().equalsIgnoreCase(RULE_WHITE));
        List<Rule> blackKeyList = fetchRuleByPredicate(values, rule -> rule.getName().equalsIgnoreCase(RULE_BLACK));
        if (CollectionUtils.isNotEmpty(whiteKeyList) && CollectionUtils.isNotEmpty(blackKeyList)) {
            log.error("{}RuleConfig of table rule , black rule ={} is invalid rule", ErrorCode.RULE_CONFIG_ERROR,
                blackKeyList);
            values.removeAll(blackKeyList);
        }
        List<Rule> rules = filterRepeatBy(values, Rule::getText);
        rules = filterInvalidRegexRule(rules, Rule::getText);
        values.clear();
        values.addAll(rules);
        return true;
    }
}
