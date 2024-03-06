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

import org.opengauss.datachecker.check.service.EndpointMetaDataManager;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.exception.CheckMetaDataException;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Objects;

/**
 * MetaDataLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/11/9
 * @since ：11
 */
@Order(100)
@Service
public class MetaDataLoader extends AbstractCheckLoader {
    @Resource
    private EndpointMetaDataManager endpointMetaDataManager;

    @Override
    public void load(CheckEnvironment checkEnvironment) {
        if (Objects.equals(checkEnvironment.getCheckMode(), CheckMode.INCREMENT)) {
            return;
        }
        try {
            int retry = 0;
            LogUtils.info(log, "check service is start to load metadata,place wait a moment.");
            if (!checkEnvironment.isCheckTableEmpty()) {
                while (endpointMetaDataManager.isMetaLoading()) {
                    ThreadUtil.sleep(retryIntervalTimes);
                    if (++retry > maxRetryTimes) {
                        LogUtils.info(log, "check service is loading metadata, try out of {}", maxRetryTimes);
                        throw new CheckMetaDataException("loading metadata, try out of " + maxRetryTimes);
                    }
                    LogUtils.info(log, "check service is loading metadata,place wait a moment.");
                }
            }
            if (endpointMetaDataManager.isMetaLoading()) {
                throw new CheckMetaDataException("check loading metadata is timeout");
            } else {
                LogUtils.info(log, "start to load metadata from source and sink.");
                endpointMetaDataManager.load();
            }
            checkEnvironment.setMetaLoading();
            LogUtils.info(log, "check service load metadata success.");
        } catch (CheckingException ex) {
            shutdown(ex.getMessage());
        }
    }
}
