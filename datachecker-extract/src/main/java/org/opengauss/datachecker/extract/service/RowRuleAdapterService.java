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

import com.alibaba.druid.pool.PreparedStatementPool;
import org.apache.commons.lang3.math.NumberUtils;
import org.opengauss.datachecker.common.entry.common.Rule;
import org.opengauss.datachecker.common.entry.extract.ConditionLimit;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.adapter.AdapterContext;
import org.opengauss.datachecker.extract.adapter.service.CheckRowRule;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * RowRuleAdapterService
 *
 * @author ：wangchao
 * @date ：Created in 2022/12/1
 * @since ：11
 */
@Service
public class RowRuleAdapterService {
    @Resource
    private ExtractProperties extractProperties;
    PreparedStatementPool statementPool;
    /**
     * Execute row-level rules
     * @param rules rules
     * @param tableMetadataMap tableMetadataMap
     */
    public void executeRowRule(List<Rule> rules, Map<String, TableMetadata> tableMetadataMap) {
        tableMetadataMap.forEach((tableName, tableMetadata) -> {
            for (Rule rule : rules) {
                if (Pattern.matches(rule.getName(), tableName)) {
                    tableMetadata.setConditionLimit(getConditionLimit(rule.getText()));
                }
            }
        });
    }

    private ConditionLimit getConditionLimit(String ruleText) {
        ConditionLimit conditionLimit = null;
        final String[] condition = ruleText.split(",");
        if (NumberUtils.isDigits(condition[0]) && NumberUtils.isDigits(condition[1])) {
            conditionLimit = new ConditionLimit(Integer.parseInt(condition[0]), Long.parseLong(condition[1]));
        }
        return conditionLimit;
    }

    private boolean checkRule(Rule rule) {
        CheckRowRule checkRowRule = AdapterContext.getBean(extractProperties.getDatabaseType(), CheckRowRule.class);
        return checkRowRule.checkRule(extractProperties.getSchema(), rule);
    }
}
