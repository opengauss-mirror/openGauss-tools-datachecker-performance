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

package org.opengauss.datachecker.extract.adapter.service;

import org.opengauss.datachecker.common.entry.common.Rule;

/**
 * CheckRowRule
 *
 * @author ：wangchao
 * @date ：Created in 2022/12/2
 * @since ：11
 */
public interface CheckRowRule {
    String CHECK_RULE_STATEMENT = "select count(1) from %s.%s where %s limit 1; ";

    /**
     * Check whether the filter rule of table rows is valid
     *
     * @param schema schema
     * @param rule   rule
     * @return true|false
     */
    boolean checkRule(String schema, Rule rule);
}
