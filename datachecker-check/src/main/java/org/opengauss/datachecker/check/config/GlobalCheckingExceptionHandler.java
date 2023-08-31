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

package org.opengauss.datachecker.check.config;

import org.opengauss.datachecker.common.entry.enums.ResultEnum;
import org.opengauss.datachecker.common.exception.*;
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
public class GlobalCheckingExceptionHandler extends GlobalCommonExceptionHandler {

    /**
     * Business exception handling
     */
    @ExceptionHandler(value = CheckingException.class)
    public Result checkingException(HttpServletRequest request, CheckingException e) {
        logError(request, e);
        return Result.fail(ResultEnum.CHECKING, e.getMessage());
    }

    @ExceptionHandler(value = CheckingAddressConflictException.class)
    public Result checkingAddressConflictException(HttpServletRequest request, CheckingAddressConflictException e) {
        logError(request, e);
        return Result.fail(ResultEnum.CHECKING_ADDRESS_CONFLICT, e.getMessage());
    }

    @ExceptionHandler(value = CheckMetaDataException.class)
    public Result checkMetaDataException(HttpServletRequest request, CheckMetaDataException e) {
        logError(request, e);
        return Result.fail(ResultEnum.CHECK_META_DATA, e.getMessage());
    }

    @ExceptionHandler(value = LargeDataDiffException.class)
    public Result largeDataDiffException(HttpServletRequest request, LargeDataDiffException e) {
        logError(request, e);
        return Result.fail(ResultEnum.LARGE_DATA_DIFF, e.getMessage());
    }

    @ExceptionHandler(value = MerkleTreeDepthException.class)
    public Result merkleTreeDepthException(HttpServletRequest request, MerkleTreeDepthException e) {
        logError(request, e);
        return Result.fail(ResultEnum.MERKLE_TREE_DEPTH, e.getMessage());
    }
}
