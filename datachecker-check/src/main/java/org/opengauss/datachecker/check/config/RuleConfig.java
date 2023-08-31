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

package org.opengauss.datachecker.check.config;

import lombok.Data;
import org.opengauss.datachecker.check.annotation.ColumnRule;
import org.opengauss.datachecker.check.annotation.RowRule;
import org.opengauss.datachecker.check.annotation.TableRule;
import org.opengauss.datachecker.common.entry.common.Rule;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import java.util.List;
import java.util.Map;

/**
 * RuleConfig
 *
 * @author ：wangchao
 * @date ：Created in 2022/11/29
 * @since ：11
 */
@Validated
@Data
@Configuration
@ConfigurationProperties(prefix = "rules", ignoreInvalidFields = true)
public class RuleConfig {
    private boolean enable;
    @TableRule
    private List<Rule> table;
    @RowRule
    private List<Rule> row;
    @ColumnRule
    private List<Rule> column;

    public void tableRuleClear() {
        table.clear();
    }

    public void rowRuleClear() {
        row.clear();
    }

    public void columnRuleClear() {
        column.clear();
    }
}
