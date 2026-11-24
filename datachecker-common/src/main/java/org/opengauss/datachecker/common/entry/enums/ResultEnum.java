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

package org.opengauss.datachecker.common.entry.enums;

import lombok.Getter;

/**
 * ResultEnum {@value API_DESCRIPTION}
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/26
 * @since ：11
 */
@Getter
public enum ResultEnum {
    /**
     * verification service exception
     */
    CHECKING(1000, "verification service exception"),

    /**
     * verify service address port conflict
     */
    CHECKING_ADDRESS_CONFLICT(1001, "verify service address port conflict."),

    /**
     * verification service meta data exception
     */
    CHECK_META_DATA(1002, "verification service meta data exception."),

    /**
     * verification service - the table data difference is too large to be verified
     */
    LARGE_DATA_DIFF(1003, "verification service -the data difference is too large to be verified."),

    /**
     * verification service - height of Merkel tree is inconsistent.
     */
    MERKLE_TREE_DEPTH(1004, "verification service - height of Merkel tree is inconsistent."),

    /**
     * extraction service exception
     */
    EXTRACT(2000, "extraction service exception:"),

    /**
     * create kafka topic exception
     */
    CREATE_TOPIC(2001, "create kafka topic exception:"),

    /**
     * The current instance is executing the data extraction service and cannot restart the new verification
     */
    PROCESS_MULTIPLE(2002, "The instance is executing and cannot restart the new verification."),

    /**
     * data extraction service, no extraction task to be executed found
     */
    TASK_NOT_FOUND(2003, "data extraction service, no extraction task to be executed found."),

    /**
     * data extraction service. The metadata corresponding to the current table does not exist.
     */
    TABLE_NOT_FOUND(2004, "The metadata corresponding to the current table does not exist."),

    /**
     * debezium configuration error
     */
    DEBEZIUM_CONFIG_ERROR(2005, "debezium configuration error"),

    /**
     * feign client exception
     */
    FEIGN_CLIENT(3000, "feign client exception"),

    /**
     * scheduling feign client exception
     */
    DISPATCH_CLIENT(3001, "scheduling feign client exception"),

    /**
     * SUCCESS
     */
    SUCCESS(200, "SUCCESS"),

    /**
     * ERROR
     */
    SERVER_ERROR(400, "ERROR"),

    /**
     * The requested data format does not match
     */
    BAD_REQUEST(400, "The requested data format does not match!"),

    /**
     * login certificate expired
     */
    UNAUTHORIZED(401, "login certificate expired!"),

    /**
     * Sorry, you have no access!
     */
    FORBIDDEN(403, "Sorry, you have no access!"),

    /**
     * The requested resource cannot be found!
     */
    NOT_FOUND(404, "The requested resource cannot be found!"),

    /**
     * Parameter missing
     */
    PARAM_MISSING(405, "Parameter missing"),

    /**
     * Parameter type mismatch
     */
    PARAM_TYPE_MISMATCH(406, "Parameter type mismatch"),

    /**
     * request method is not supported
     */
    HTTP_REQUEST_METHOD_NOT_SUPPORTED_ERROR(407, "request method is not supported"),

    /**
     * illegal parameter exception
     */
    SERVER_ERROR_PRARM(408, "illegal parameter exception"),

    /**
     * server internal error
     */
    INTERNAL_SERVER_ERROR(500, "server internal error!"),

    /**
     * the server is busy, please try again later!
     */
    SERVICE_UNAVAILABLE(503, "the server is busy, please try again later!"),

    /**
     * Unknown exception!"
     */
    UNKNOWN(7000, "Unknown exception!");

    private final int code;
    private final String description;

    ResultEnum(int code, String description) {
        this.code = code;
        this.description = description;
    }

}
