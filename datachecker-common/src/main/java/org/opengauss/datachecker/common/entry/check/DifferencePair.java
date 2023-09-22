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

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/1
 * @since ：11
 */
@Getter
@EqualsAndHashCode
public final class DifferencePair<L, R, D> {

    private L onlyOnLeft;
    private R onlyOnRight;
    private D differing;

    private DifferencePair(L onlyOnLeft, R onlyOnRight, D differing) {
        this.onlyOnLeft = onlyOnLeft;
        this.onlyOnRight = onlyOnRight;
        this.differing = differing;
    }

    /**
     * Creates a new {@link DifferencePair} for the given elements.
     *
     * @param onlyOnLeft  must not be {@literal null}.
     * @param onlyOnRight must not be {@literal null}.
     * @param differing   must not be {@literal null}.
     * @return
     */
    public static <L, R, D> DifferencePair<L, R, D> of(L onlyOnLeft, R onlyOnRight, D differing) {
        return new DifferencePair<>(onlyOnLeft, onlyOnRight, differing);
    }

    public void clear() {
        this.onlyOnLeft = null;
        this.differing = null;
        this.onlyOnRight = null;
    }

    /**
     * @return differing : onlyOnLeft -> onlyOnRight
     */
    @Override
    public String toString() {
        return String.format("%s : %s->%s", this.differing, this.onlyOnLeft, this.onlyOnRight);
    }
}
