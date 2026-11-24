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

package org.opengauss.datachecker.extract.adapter.service.mysql;

import org.opengauss.datachecker.extract.adapter.service.CheckRowRuleService;
import org.springframework.stereotype.Service;

/**
 * MysqlCheckRowRule
 *
 * @author ：wangchao
 * @date ：Created in 2022/12/2
 * @since ：11
 */
@Service
public class MysqlCheckRowRule extends CheckRowRuleService {

    @Override
    protected String convert(String text) {
        return "`" + text + "`";
    }

    @Override
    protected String convertCondition(String text) {
        return text;
    }
}
