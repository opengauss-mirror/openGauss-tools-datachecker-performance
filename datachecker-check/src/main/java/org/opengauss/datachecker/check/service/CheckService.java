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

package org.opengauss.datachecker.check.service;

import org.opengauss.datachecker.common.entry.enums.CheckMode;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
public interface CheckService {

    /**
     * Enable verification service
     *
     * @param checkMode checkMode
     * @return Process number
     */
    String start(CheckMode checkMode);

    /**
     * Query the currently executed process number
     *
     * @return Process number
     */
    String getCurrentCheckProcess();

    /**
     * Clean up the verification environment
     */
    void cleanCheck();
}
