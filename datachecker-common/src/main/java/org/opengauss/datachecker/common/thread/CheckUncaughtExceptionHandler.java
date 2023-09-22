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

package org.opengauss.datachecker.common.thread;

import org.apache.logging.log4j.Logger;

/**
 * CheckUncaughtExceptionHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/11/17
 * @since ：11
 */
public class CheckUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
    private final Logger logger;

    /**
     * CheckUncaughtExceptionHandler
     *
     * @param logger logger
     */
    public CheckUncaughtExceptionHandler(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        String msg = String.format("getException from thread: %s,exceptionName:%s", t.getName(), e.getMessage());
        logger.error(msg, e);
    }
}
