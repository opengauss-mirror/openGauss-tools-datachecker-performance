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
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Map;

/**
 * EnvironmentLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/12/2
 * @since ：11
 */
@Slf4j
@Service
public class EnvironmentLoader {
    @Resource
    private volatile ExtractEnvironment extractEnvironment;

    @Async
    public void load(CheckMode checkMode) {
        log.info("extract environment loader start");
        extractEnvironment.setCheckMode(checkMode);
        final Map<String, ExtractLoader> beans = SpringUtil.getBeans(ExtractLoader.class);
        beans.values().forEach(loader -> {
            log.info("extract environment loader {} start", loader.getClass().getName());
            loader.load(extractEnvironment);
        });
    }
}
