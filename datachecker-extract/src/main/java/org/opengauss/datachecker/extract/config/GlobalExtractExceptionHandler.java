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

package org.opengauss.datachecker.extract.config;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.enums.ResultEnum;
import org.opengauss.datachecker.common.exception.*;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import javax.servlet.http.HttpServletRequest;

/**
 * GlobalCheckingExceptionHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/1
 * @since ：11
 */
@RestControllerAdvice
public class GlobalExtractExceptionHandler extends GlobalCommonExceptionHandler {
    private static final String MESSAGE_TEMPLATE = "path:{}, queryParam:[{}] , error:";
    private static final Logger log = LogUtils.getLogger();
    /**
     * service ExtractException exception handing
     *
     * @param request   request
     * @param exception exception
     * @return request result
     */
    @ExceptionHandler(value = ExtractException.class)
    public Result extractException(HttpServletRequest request, ExtractException exception) {
        logError(request, exception);
        return Result.fail(ResultEnum.EXTRACT, exception.getMessage());
    }

    /**
     * service CreateTopicException exception handing
     *
     * @param request   request
     * @param exception exception
     * @return request result
     */
    @ExceptionHandler(value = CreateTopicException.class)
    public Result createTopicException(HttpServletRequest request, CreateTopicException exception) {
        logError(request, exception);
        return Result.fail(ResultEnum.CREATE_TOPIC, exception.getMessage());
    }

    /**
     * service ProcessMultipleException exception handing
     *
     * @param request   request
     * @param exception exception
     * @return request result
     */
    @ExceptionHandler(value = ProcessMultipleException.class)
    public Result processMultipleException(HttpServletRequest request, ProcessMultipleException exception) {
        logError(request, exception);
        return Result.fail(ResultEnum.PROCESS_MULTIPLE, exception.getMessage());
    }

    /**
     * service TaskNotFoundException exception handing
     *
     * @param request   request
     * @param exception exception
     * @return request result
     */
    @ExceptionHandler(value = TaskNotFoundException.class)
    public Result taskNotFoundException(HttpServletRequest request, TaskNotFoundException exception) {
        logError(request, exception);
        return Result.fail(ResultEnum.TASK_NOT_FOUND, exception.getMessage());
    }

    /**
     * service TableNotExistException exception handing
     *
     * @param request   request
     * @param exception exception
     * @return request result
     */
    @ExceptionHandler(value = TableNotExistException.class)
    public Result tableNotExistException(HttpServletRequest request, TableNotExistException exception) {
        logError(request, exception);
        return Result.fail(ResultEnum.TABLE_NOT_FOUND, exception.getMessage());
    }

    /**
     * service DebeziumConfigException exception handing
     *
     * @param request   request
     * @param exception exception
     * @return request result
     */
    @ExceptionHandler(value = DebeziumConfigException.class)
    public Result debeziumConfigException(HttpServletRequest request, DebeziumConfigException exception) {
        logError(request, exception);
        return Result.fail(ResultEnum.DEBEZIUM_CONFIG_ERROR, exception.getMessage());
    }

    /**
     * service DebeziumHandlerException exception handing
     *
     * @param request   request
     * @param exception exception
     * @return request result
     */
    @ExceptionHandler(value = DebeziumHandlerException.class)
    public Result debeziumHandlerException(HttpServletRequest request, DebeziumHandlerException exception) {
        logError(request, exception);
        return Result.fail(ResultEnum.DEBEZIUM_CONFIG_ERROR, exception.getMessage());
    }

    private void logError(HttpServletRequest request, ExtractException exception) {
        log.error(MESSAGE_TEMPLATE, request.getRequestURI(), request.getQueryString(), exception);
    }
}
