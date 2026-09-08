/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
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

package org.opengauss.datachecker.common.entry.enums;

import lombok.Getter;

/**
 * Comparison precision mode of the check result.
 *
 * @author: xujintao
 * @date: Created in 2026/9/7
 * @since: 11
 */
@Getter
public enum PrecisionMode implements IEnum {
    /**
     * Strict comparison: keeps full precision (TIMESTAMP keeps sub-second digits, NUMBER uses toPlainString,
     * FLOAT uses double). Suitable for detecting precision drift during migration.
     */
    STRICT("STRICT", "exact comparison"),
    /**
     * Kernel-compatible: accepts some precision loss to tolerate kernel differences between both ends
     * (TIMESTAMP truncated to seconds, NUMBER uses double/BigInteger, FLOAT uses 32-bit).
     * Default mode.
     */
    COMPATIBLE("COMPATIBLE", "kernel compatible");

    private final String code;
    private final String description;

    PrecisionMode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    /**
     * PrecisionMode api description
     */
    public static final String API_DESCRIPTION = "PrecisionMode [STRICT-exact comparison,COMPATIBLE-kernel compatible]";
}
