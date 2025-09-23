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

package org.opengauss.datachecker.common.web;

import com.alibaba.fastjson.annotation.JSONType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.opengauss.datachecker.common.entry.enums.ResultEnum;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/26
 * @since ：11
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JSONType(orders = {"code", "message", "data"})
public class Result<T> {
    private int code;
    private String message;
    private T data;

    public static <T> Result<T> success() {
        return new Result<>(ResultEnum.SUCCESS.getCode(), ResultEnum.SUCCESS.getDescription(), null);
    }

    public static <T> Result<T> of(T data) {
        return new Result<>(ResultEnum.SUCCESS.getCode(), ResultEnum.SUCCESS.getDescription(), data);
    }

    public static <T> Result<T> of(T data, int code, String message) {
        return new Result<>(code, message, data);
    }

    public static <T> Result<T> fail(ResultEnum resultEnum) {
        return new Result(resultEnum.getCode(), resultEnum.getDescription(), null);
    }

    public static <T> Result<T> fail(ResultEnum resultEnum, String message) {
        return new Result(resultEnum.getCode(), resultEnum.getDescription() + " " + message, null);
    }

    public boolean isSuccess() {
        return code == ResultEnum.SUCCESS.getCode();
    }

    public static <T> Result<T> success(T data) {
        Result<T> result = new Result<>();
        result.setCode(ResultEnum.SUCCESS.getCode());
        result.setMessage(ResultEnum.SUCCESS.getDescription());
        result.setData(data);
        return result;
    }

    public static <T> Result<T> error(String message) {
        Result<T> result = new Result<>();
        result.setCode(ResultEnum.SERVER_ERROR.getCode());
        result.setMessage(message);
        return result;
    }
}
