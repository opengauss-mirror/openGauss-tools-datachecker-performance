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

import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.check.annotation.ColumnRule;
import org.opengauss.datachecker.common.entry.common.Rule;

import jakarta.validation.ConstraintValidatorContext;
import java.util.List;
import java.util.function.Predicate;

import static org.opengauss.datachecker.check.config.validator.RuleConfigValidatorUtil.fetchRuleByPredicate;
import static org.opengauss.datachecker.check.config.validator.RuleConfigValidatorUtil.filterRepeatBy;

/**
 * RuleConfigColumnValidator
 * column:
 * - name: t_test_1
 *   text: id,portal_id,func_id,name,width,last_upd_time
 *   attribute: include
 * - name: t_test_2
 *   text: name,height,last_upd_time
 *   attribute: exclude
 *
 * @author ：wangchao
 * @date ：Created in 2022/12/7
 * @since ：11
 */
public class RuleConfigColumnValidator implements RuleConfigValidator<ColumnRule> {
    private static final String INCLUDE = "include";
    private static final String EXCLUDE = "exclude";
    private static final Predicate<Rule> EMPTY_RULE_PREDICATE =
        rule -> StringUtils.isNotBlank(rule.getName()) || StringUtils.isNotBlank(rule.getText());
    private static final Predicate<Rule> ATTR_RULE_PREDICATE =
        rule -> StringUtils.equalsIgnoreCase(rule.getAttribute(), INCLUDE) || StringUtils
            .equalsIgnoreCase(rule.getAttribute(), EXCLUDE);

    /**
     * To configure column-level rules, users should follow the yaml syntax.
     * The corresponding check and filtering of a single rule will be triggered
     * when the extraction end adapts to the rule.
     * The table name and table field are not repeatedly here，we filter the repeatedly bu rule name
     *
     * @param values  column rules
     * @param context validator context
     * @return
     */
    @Override
    public boolean isValid(List<Rule> values, ConstraintValidatorContext context) {
        List<Rule> filterEmptyList = fetchRuleByPredicate(values, EMPTY_RULE_PREDICATE);
        List<Rule> filterInvalidAttrRules = fetchRuleByPredicate(filterEmptyList, ATTR_RULE_PREDICATE);
        List<Rule> rules = filterRepeatBy(filterInvalidAttrRules, Rule::getName);
        values.clear();
        values.addAll(rules);
        return true;
    }
}
