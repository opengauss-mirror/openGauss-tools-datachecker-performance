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

package org.opengauss.datachecker.common.exception;

/**
 * The current instance is executing the data extraction service and cannot restart the new verification.
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class ProcessMultipleException extends ExtractException {
    private static final long serialVersionUID = -5298809357642777004L;

    public ProcessMultipleException(String message) {
        super(message);
    }

}
