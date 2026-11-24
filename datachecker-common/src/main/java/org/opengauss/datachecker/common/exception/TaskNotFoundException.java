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

/**
 * Data extraction service, no extraction task to be executed found
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class TaskNotFoundException extends ExtractException {
    private static final long serialVersionUID = -3242004357180803240L;
    private static final String ERROR_MESSAGE = "task %s is not found,please checking something error!";
    private static final String ERROR_ENDPOINT_MESSAGE = "endpoint [%s] and process[%s] task is empty!";

    public TaskNotFoundException(Endpoint endpoint, long process) {
        super(String.format(ERROR_ENDPOINT_MESSAGE, endpoint.getDescription(), process));
    }

    public TaskNotFoundException(String taskName) {
        super(String.format(ERROR_MESSAGE, taskName));
    }

}
