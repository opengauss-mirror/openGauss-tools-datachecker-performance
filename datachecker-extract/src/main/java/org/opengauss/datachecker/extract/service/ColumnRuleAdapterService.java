/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-tools-datachecker-performance is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.extract.service;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.common.constant.RuleConstants;
import org.opengauss.datachecker.common.entry.common.Rule;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * ColumnRuleAdapterService
 *
 * @author ：wangchao
 * @date ：Created in 2022/12/1
 * @since ：11
 */
@Service
public class ColumnRuleAdapterService {
    private static final String SPLIT = RuleConstants.RULE_SPLIT;
    private static final String EXCLUDE = RuleConstants.RULE_EXCLUDE;

    public List<ColumnsMetaData> executeColumnRule(List<Rule> rules, List<ColumnsMetaData> columns) {
        if (CollectionUtils.isEmpty(rules)) {
            return columns;
        }
        return filterColumnByRule(columns, rules);
    }

    private List<ColumnsMetaData> filterColumnByRule(List<ColumnsMetaData> columns, List<Rule> rules) {
        if (CollectionUtils.isEmpty(rules)) {
            return columns;
        }
        Map<String, Rule> ruleMap = translateColumnRules(rules);
        Map<String, List<String>> ruleTextsMap = translateColumnRule(rules);
        return columns.parallelStream().filter(metaData -> {
            if (!ruleMap.containsKey(metaData.getTableName())) {
                return true;
            }
            if (Objects.equals(metaData.getColumnKey(), ColumnKey.PRI)) {
                return true;
            } else {
                final Rule rulesOfTable = ruleMap.get(metaData.getTableName());
                final List<String> ruleTextsOfTable = ruleTextsMap.get(metaData.getTableName());
                if (StringUtils.equals(rulesOfTable.getAttribute(), EXCLUDE)) {
                    return !ruleTextsOfTable.contains(metaData.getColumnName().toLowerCase(Locale.ENGLISH));
                } else {
                    return ruleTextsOfTable.contains(metaData.getColumnName().toLowerCase(Locale.ENGLISH));
                }
            }

        }).collect(Collectors.toList());
    }

    private Map<String, Rule> translateColumnRules(List<Rule> rules) {
        return rules.stream().collect(Collectors.toMap(Rule::getName, Function.identity()));
    }

    private Map<String, List<String>> translateColumnRule(List<Rule> rules) {
        Map<String, List<String>> ruleMap = new HashMap<>();
        rules.forEach(rule -> {
            ruleMap.put(rule.getName(), Arrays.asList(rule.getText().toLowerCase(Locale.ENGLISH).split(SPLIT)));
        });
        return ruleMap;
    }
}
