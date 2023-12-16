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

package org.opengauss.datachecker.common.entry.common;

import lombok.Data;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.RuleType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author ：wangchao
 * @date ：Created in 2023/3/16
 * @since ：11
 */
@Data
public class GlobalConfig {
    private CheckMode checkMode;
    private String processPath;
    private Map<String, Object> commonConfig;
    private Map<RuleType, List<Rule>> rules;

    public void addCommonConfig(String key, Object value) {
        if (commonConfig == null) {
            commonConfig = new HashMap<>();
        }
        commonConfig.put(key, value);
    }
}
