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

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.entry.enums.ResultEnum;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import javax.servlet.http.HttpServletRequest;

/**
 * GlobalCommonExceptionHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class GlobalCommonExceptionHandler {
    private static final Logger log = LogUtils.getLogger();

    /**
     * Missing required parameters
     *
     * @param request request
     * @param exp     exception
     * @return method request result
     */
    @ExceptionHandler(value = MissingServletRequestParameterException.class)
    public Result missingParameterHandler(HttpServletRequest request, MissingServletRequestParameterException exp) {
        logError(request, exp);
        return Result.fail(ResultEnum.PARAM_MISSING);
    }

    /**
     * Parameter type mismatch
     *
     * @param request request
     * @param exp     exception
     * @return method request result
     */
    @ExceptionHandler(value = MethodArgumentTypeMismatchException.class)
    public Result methodArgumentTypeMismatchException(HttpServletRequest request,
        MethodArgumentTypeMismatchException exp) {
        logError(request, exp);
        return Result.fail(ResultEnum.PARAM_TYPE_MISMATCH);
    }

    /**
     * Unsupported request method
     *
     * @param request request
     * @param exp     exception
     * @return method request result
     */
    @ExceptionHandler(value = HttpRequestMethodNotSupportedException.class)
    public Result httpRequestMethodNotSupportedException(HttpServletRequest request,
        HttpRequestMethodNotSupportedException exp) {
        logError(request, exp);
        return Result.fail(ResultEnum.HTTP_REQUEST_METHOD_NOT_SUPPORTED_ERROR);
    }

    /**
     * bad parameter
     *
     * @param request request
     * @param exp     exception
     * @return method request result
     */
    @ExceptionHandler(value = IllegalArgumentException.class)
    public Result illegalArgumentException(HttpServletRequest request, IllegalArgumentException exp) {
        logError(request, exp);
        return Result.fail(ResultEnum.SERVER_ERROR_PRARM);
    }

    /**
     * FeignClientException
     *
     * @param request request
     * @param exp     exception
     * @return method request result
     */
    @ExceptionHandler(value = FeignClientException.class)
    public Result feignClientException(HttpServletRequest request, FeignClientException exp) {
        logError(request, exp);
        return Result.fail(ResultEnum.FEIGN_CLIENT);
    }

    /**
     * DispatchClientException
     *
     * @param request request
     * @param exp     exception
     * @return method request result
     */
    @ExceptionHandler(value = DispatchClientException.class)
    public Result dispatchClientException(HttpServletRequest request, DispatchClientException exp) {
        logError(request, exp);
        return Result.fail(ResultEnum.DISPATCH_CLIENT);
    }

    /**
     * Unified handling of other exceptions
     *
     * @param request request
     * @param exp     exception
     * @return method request result
     */
    @ExceptionHandler(value = Exception.class)
    public Result exception(HttpServletRequest request, Exception exp) {
        logError(request, exp);
        return Result.fail(ResultEnum.SERVER_ERROR);
    }

    /**
     * Log errors
     *
     * @param request request
     * @param exp exception
     */
    protected void logError(HttpServletRequest request, Exception exp) {
        log.error("{}path:{}, queryParam:{}, errorMessage:{}", ErrorCode.UNKNOWN, request.getRequestURI(),
            request.getQueryString(), exp.getMessage(), exp);
    }
}
