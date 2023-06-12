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

package org.opengauss.datachecker.extract.load;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.util.SpringUtil;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * EnvironmentLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/12/2
 * @since ：11
 */
@Slf4j
@Service
@ConditionalOnBean({ExtractDatabaseLoader.class, MemoryMonitorLoader.class, ThreadPoolLoader.class})
public class EnvironmentLoader {
    @Resource
    private volatile ExtractEnvironment extractEnvironment;

    @Async
    public void load(CheckMode checkMode) {
        log.info("extract environment loader start");
        extractEnvironment.setCheckMode(checkMode);
        Map<String, ExtractLoader> loaders = SpringUtil.getBeans(ExtractLoader.class);
        List<ExtractLoader> orderedLoaders = loaders.values().stream().sorted((b1, b2) -> {
            Order annotation1 = b1.getClass().getAnnotation(Order.class);
            Order annotation2 = b2.getClass().getAnnotation(Order.class);
            int order1 = Objects.isNull(annotation1) ? 1000 : annotation1.value();
            int order2 = Objects.isNull(annotation2) ? 1000 : annotation2.value();
            return order1 - order2;
        }).collect(Collectors.toList());
        orderedLoaders.stream().forEach(loader -> {
            log.info("extract environment loader {} start", loader.getClass().getName());
            loader.load(extractEnvironment);
        });
    }
}
