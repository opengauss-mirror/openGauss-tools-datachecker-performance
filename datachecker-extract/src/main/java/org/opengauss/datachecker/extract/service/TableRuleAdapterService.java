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

package org.opengauss.datachecker.extract.service;

import org.apache.commons.collections4.CollectionUtils;
import org.opengauss.datachecker.common.constant.RuleConstants;
import org.opengauss.datachecker.common.entry.common.Rule;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * TableRuleAdapterService
 *
 * @author ：wangchao
 * @date ：Created in 2022/12/1
 * @since ：11
 */
@Service
public class TableRuleAdapterService {
    private static final String WHITE = RuleConstants.RULE_WHITE;
    private static final String BLACK = RuleConstants.RULE_BLACK;
    private static final Map<String, TableRuleExecutor> EXECUTORS = new HashMap<>();
    private static final Map<String, TableRealNameRuleExecutor> REAL_NAME_EXECUTORS = new HashMap<>();

    static {
        EXECUTORS.put(WHITE,
            (patterns, table) -> patterns.stream().anyMatch(pattern -> pattern.matcher(table).matches()));
        EXECUTORS.put(BLACK,
            (patterns, table) -> patterns.stream().noneMatch(pattern -> pattern.matcher(table).matches()));
        REAL_NAME_EXECUTORS.put(WHITE,
            (patterns, table) -> patterns.stream().anyMatch(pattern -> pattern.contains(table)));
        REAL_NAME_EXECUTORS.put(BLACK,
            (patterns, table) -> patterns.stream().noneMatch(pattern -> pattern.contains(table)));
    }

    /**
     * executeTableRule
     *
     * @param rules rules
     * @param tableList tableList
     * @return filter list
     */
    public List<String> executeTableRule(List<Rule> rules, List<String> tableList) {
        if (CollectionUtils.isEmpty(rules)) {
            return tableList;
        }
        final Rule ruleOne = rules.get(0);
        final TableRuleExecutor tableRuleExecutor = EXECUTORS.get(ruleOne.getName());
        final List<Pattern> patterns = buildRulePatterns(rules);
        return tableList.parallelStream()
            .filter(table -> tableRuleExecutor.apply(patterns, table))
            .collect(Collectors.toList());
    }

    /**
     * executeTableRule
     *
     * @param rules rules
     * @param table table
     * @return filter result
     */
    public boolean executeTableRule(List<Rule> rules, String table) {
        if (CollectionUtils.isEmpty(rules)) {
            return true;
        }
        final Rule ruleOne = rules.get(0);
        if (ruleOne.getText().contains(",")) {
            List<String> tableRules = buildRealNameRule(rules);
            TableRealNameRuleExecutor realNameRuleExecutor = REAL_NAME_EXECUTORS.get(ruleOne.getName());
            return realNameRuleExecutor.apply(tableRules, table);
        }
        final TableRuleExecutor tableRuleExecutor = EXECUTORS.get(ruleOne.getName());
        final List<Pattern> patterns = buildRulePatterns(rules);
        return tableRuleExecutor.apply(patterns, table);
    }

    private List<Pattern> buildRulePatterns(List<Rule> rules) {
        return rules.stream().map(rule -> Pattern.compile(rule.getText())).collect(Collectors.toList());
    }

    private List<String> buildRealNameRule(List<Rule> rules) {
        return rules.stream()
            .map(Rule::getText)
            .map(text -> text.split(","))
            .map(Arrays::asList)
            .flatMap(List::stream)
            .collect(Collectors.toList());
    }

    @FunctionalInterface
    interface TableRuleExecutor {
        boolean apply(List<Pattern> tablePatterns, String table);
    }

    @FunctionalInterface
    interface TableRealNameRuleExecutor {
        /**
         * Judge whether it matches according to the rule and the table name
         *
         * @param tablePatterns tablePatterns
         * @param table table
         * @return boolean
         */
        boolean apply(List<String> tablePatterns, String table);
    }
}
