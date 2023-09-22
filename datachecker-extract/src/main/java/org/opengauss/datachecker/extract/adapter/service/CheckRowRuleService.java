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

package org.opengauss.datachecker.extract.adapter.service;

import org.opengauss.datachecker.common.entry.common.Rule;

/**
 * CheckRowRuleService
 *
 * @author ：wangchao
 * @date ：Created in 2022/12/2
 * @since ：11
 */
public abstract class CheckRowRuleService implements CheckRowRule {

    @Override
    public boolean checkRule(String schema, Rule rule) {
        return true;
    }

    /**
     * convert text
     *
     * @param text text
     * @return text
     */
    protected abstract String convert(String text);

    /**
     * Row level rule condition semantics support ( > < >= <= = !=) Six types of conditional filtering
     * and compound conditional statements;
     * Composite conditional statements must be spliced with and, for example, a>1 and b>2;
     *
     * @param text text
     * @return condition
     */
    protected abstract String convertCondition(String text);
}
