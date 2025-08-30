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

import org.opengauss.datachecker.check.config.DataCheckProperties;
import org.opengauss.datachecker.check.modules.check.ExportCheckResult;
import org.opengauss.datachecker.check.service.IncrementLogManager;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;

/**
 * CheckResultPathLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/10/31
 * @since ：11
 */
@Order(96)
@Service
public class CheckResultPathLoader extends AbstractCheckLoader {
    @Resource
    private DataCheckProperties properties;
    @Resource
    private IncrementLogManager incrementLogManager;

    /**
     * Initialize the verification result environment
     */
    @Override
    public void load(CheckEnvironment checkEnvironment) {
        ExportCheckResult.initEnvironment(properties.getDataPath());
        checkEnvironment.setExportCheckPath(properties.getDataPath());
        ExportCheckResult.backCheckResultDirectory();
        incrementLogManager.init(ExportCheckResult.getResultBakRootDir());
        LogUtils.info(log, "check service load export environment success.");
    }
}
