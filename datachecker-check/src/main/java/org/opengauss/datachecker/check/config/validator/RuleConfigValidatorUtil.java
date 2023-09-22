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
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.common.Rule;
import org.opengauss.datachecker.common.util.LogUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toCollection;

/**
 * RuleConfigValidatorUtil
 *
 * @author ：wangchao
 * @date ：Created in 2022/12/7
 * @since ：11
 */
public class RuleConfigValidatorUtil {
    private static final Logger log = LogUtils.getLogger();
    public static List<Rule> fetchRuleByPredicate(List<Rule> rules, Predicate<Rule> rulePredicate) {
        if (CollectionUtils.isEmpty(rules)) {
            return new ArrayList<>();
        }
        return rules.stream().filter(rulePredicate).distinct().collect(Collectors.toList());
    }

    public static List<Rule> filterRepeatBy(List<Rule> rules, Function<Rule, String> keyExtractor) {
        if (CollectionUtils.isEmpty(rules)) {
            return new ArrayList<>();
        }
        return rules.stream().collect(
            collectingAndThen(toCollection(() -> new TreeSet<>(Comparator.comparing(keyExtractor))), ArrayList::new));
    }

    public static List<Rule> filterInvalidRegexRule(List<Rule> rules, Function<Rule, String> ruleRegexExtractor) {
        if (CollectionUtils.isEmpty(rules)) {
            return new ArrayList<>();
        }
        return rules.stream().filter(rule -> isValidRuleRegex(rule, ruleRegexExtractor)).collect(Collectors.toList());
    }

    private static boolean isValidRuleRegex(Rule rule, Function<Rule, String> ruleRegexExtractor) {
        final String ruleRegex = ruleRegexExtractor.apply(rule);
        if (StringUtils.isBlank(ruleRegex)) {
            return false;
        }
        try {
            Pattern.compile(ruleRegex);
            return true;
        } catch (PatternSyntaxException ex) {
            log.error("RuleConfig of table rule , {} is invalid rule", rule);
            return false;
        }
    }
}
