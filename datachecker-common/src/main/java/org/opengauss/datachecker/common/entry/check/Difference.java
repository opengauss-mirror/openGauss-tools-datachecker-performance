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

package org.opengauss.datachecker.common.entry.check;

import lombok.Data;

/**
 * Difference
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/26
 * @since ：11
 */
@Data
public class Difference {
    private String key;
    private int idx;

    public Difference(String key, int idx) {
        this.key = key;
        this.idx = idx;
    }

    @Override
    public String toString() {
        return "(key=" + key + ", idx=" + idx + ")";
    }
}
