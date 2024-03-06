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

package org.opengauss.datachecker.check.load;

import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.DataLoad;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ExtractConfig;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Objects;

/**
 * CheckModeLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/10/31
 * @since ：11
 */
@Order(98)
@Service
public class CheckModeLoader extends AbstractCheckLoader {
    @Resource
    private FeignClientService feignClient;

    /**
     * Initialize the verification result environment
     */
    @Override
    public void load(CheckEnvironment checkEnvironment) {
        final ExtractConfig sourceConfig = feignClient.getEndpointConfig(Endpoint.SOURCE);
        if (sourceConfig.isDebeziumEnable()) {
            checkEnvironment.setCheckMode(CheckMode.INCREMENT);
        } else {
            CheckMode checkMode;
            if (Objects.equals(sourceConfig.getDataLoadMode(), DataLoad.CSV)) {
                checkMode = CheckMode.CSV;
            } else {
                checkMode = CheckMode.FULL;
            }
            checkEnvironment.setCheckMode(checkMode);
        }
        ConfigCache.put(ConfigConstants.CHECK_MODE, checkEnvironment.getCheckMode());
        LogUtils.info(log, "check service load check mode success.");
    }
}
