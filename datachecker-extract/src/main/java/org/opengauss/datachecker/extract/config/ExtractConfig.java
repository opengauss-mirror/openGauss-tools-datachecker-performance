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

package org.opengauss.datachecker.extract.config;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * ExtractConfig
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/23
 * @since ：11
 */
@Component
public class ExtractConfig {
    private static final Logger log = LogUtils.getLogger(ExtractConfig.class);

    @Autowired
    private ExtractProperties extractProperties;

    /**
     * Start loading check config properties
     */
    @PostConstruct
    public void initLoad() {
        LogUtils.info(log, "check config properties [{}]", JsonObjectUtil.format(extractProperties));
    }
}
