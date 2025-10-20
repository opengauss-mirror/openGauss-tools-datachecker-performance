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

package org.opengauss.datachecker.common.exception;

import java.util.Objects;

/**
 * CheckingException
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class CheckingException extends RuntimeException {
    private static final long serialVersionUID = -5335567756924351615L;

    private final String msg;

    public CheckingException(String message) {
        msg = message;
    }

    /**
     * check exception
     *
     * @param message message
     * @param throwable throwable
     */
    public CheckingException(String message, Throwable throwable) {
        super(message, throwable);
        msg = message;
    }

    @Override
    public String getMessage() {
        String message = super.getMessage();
        if (Objects.isNull(message)) {
            return msg;
        }
        return msg + message;
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
