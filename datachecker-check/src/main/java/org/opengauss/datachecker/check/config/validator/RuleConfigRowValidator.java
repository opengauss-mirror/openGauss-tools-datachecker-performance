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
import org.opengauss.datachecker.check.annotation.RowRule;
import org.opengauss.datachecker.common.entry.common.Rule;

import jakarta.validation.ConstraintValidatorContext;
import java.util.List;
import java.util.regex.Pattern;

import static org.opengauss.datachecker.check.config.validator.RuleConfigValidatorUtil.fetchRuleByPredicate;
import static org.opengauss.datachecker.check.config.validator.RuleConfigValidatorUtil.filterRepeatBy;
import static org.opengauss.datachecker.check.config.validator.RuleConfigValidatorUtil.filterInvalidRegexRule;

/**
 * RuleConfigRowValidator
 * row:
 * - name: ^[a-zA-Z][a-zA-Z_]+$
 *   text: 10,100
 * - name: ^[a-zA-Z][a-zA-Z_]+$
 *   text: 10,100
 *
 * @author ：wangchao
 * @date ：Created in 2022/12/7
 * @since ：11
 */
public class RuleConfigRowValidator implements RuleConfigValidator<RowRule> {

    @Override
    public boolean isValid(List<Rule> value, ConstraintValidatorContext context) {
        isRowValid(value, ROW_PATTERN);
        return true;
    }

    public void isRowValid(List<Rule> values, Pattern rowPattern) {
        if (CollectionUtils.isEmpty(values)) {
            return;
        }
        List<Rule> rules = filterRepeatBy(values, Rule::getName);
        rules = filterInvalidRegexRule(rules, Rule::getName);
        rules = fetchRuleByPredicate(rules, rule -> rowPattern.matcher(rule.getText()).matches());
        values.clear();
        values.addAll(rules);
    }
}
