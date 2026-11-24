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

import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.springframework.lang.NonNull;

/**
 * Tool feignclient call exception
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class FeignClientException extends RuntimeException {
    private static final long serialVersionUID = 4698075893341122469L;

    public FeignClientException(@NonNull Endpoint endpoint, String message) {
        super(endpoint.getDescription() + " " + message);
    }

}
