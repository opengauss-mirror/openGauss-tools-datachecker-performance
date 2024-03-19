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

package org.opengauss.datachecker.common.entry.common;

import lombok.Data;

/**
 * Health
 *
 * @author ：wangchao
 * @date ：Created in 2024/3/19
 * @since ：11
 */
@Data
public class Health {
    boolean health;
    String message;

    public Health(boolean health, String message) {
        this.health = health;
        this.message = message;
    }

    /**
     * 构建健康状态结果
     *
     * @return result
     */
    public static Health buildSuccess() {
        return new Health(true, "server is start success");
    }

    /**
     * 构建异常状态结果
     *
     * @param reason 异常原因
     * @return result
     */
    public static Health buildFailed(String reason) {
        return new Health(false, reason);
    }
}
