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
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import java.util.Objects;

/**
 * EmptyDataBaseCheckLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/11/9
 * @since ：11
 */
@Order(99)
@Service
public class EmptyDataBaseCheckLoader extends AbstractCheckLoader {
    @Resource
    private EndpointMetaDataManager endpointMetaDataManager;

    @Override
    public void load(CheckEnvironment checkEnvironment) {
        if (Objects.equals(checkEnvironment.getCheckMode(), CheckMode.INCREMENT)) {
            return;
        }
        try {
            checkEnvironment.setCheckTableEmpty(
                endpointMetaDataManager.isCheckTableEmpty(Endpoint.SINK) && endpointMetaDataManager.isCheckTableEmpty(
                    Endpoint.SOURCE));
            if (checkEnvironment.isCheckTableEmpty()) {
                LogUtils.info(log, "check database table is empty.");
            }
        } catch (CheckingException ex) {
            shutdown(ex.getMessage());
        }
    }
}
