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

package org.opengauss.datachecker.extract.adapter;

import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.util.SpringUtil;
import org.opengauss.datachecker.extract.adapter.service.CheckRowRule;

import java.util.HashMap;
import java.util.Map;

/**
 * AdapterContext
 *
 * @author ：wangchao
 * @date ：Created in 2022/12/2
 * @since ：11
 */
public class AdapterContext {
    private static final Map<DataBaseType, Map<Class<?>, String>> CONTEXT = new HashMap<>();

    static {
        CONTEXT.put(DataBaseType.MS, Map.ofEntries(Map.entry(CheckRowRule.class, "mysqlCheckRowRule")));
        CONTEXT.put(DataBaseType.OG, Map.ofEntries(Map.entry(CheckRowRule.class, "openGaussCheckRowRule")));
        CONTEXT.put(DataBaseType.OGRAC, Map.ofEntries(Map.entry(CheckRowRule.class, "openGaussCheckRowRule")));
    }

    public static <T> T getBean(DataBaseType databaseType, Class<T> classz) {
        final String beanName = CONTEXT.get(databaseType).get(classz);
        return SpringUtil.getBean(beanName, classz);
    }
}
