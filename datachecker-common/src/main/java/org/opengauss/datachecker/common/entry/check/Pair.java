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

package org.opengauss.datachecker.common.entry.check;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.springframework.lang.NonNull;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/1
 * @since ：11
 */
@Getter
@EqualsAndHashCode
public final class Pair<S, T> {
    private S source;
    private T sink;

    private Pair(S source, T sink) {
        this.source = source;
        this.sink = sink;
    }

    /**
     * Creates a new {@link Pair} for the given elements.
     *
     * @param source must not be {@literal null}.
     * @param sink   must not be {@literal null}.
     * @return
     */
    public static <S, T> Pair<S, T> of(S source, T sink) {
        return new Pair<>(source, sink);
    }

    /**
     * modify pair sink
     *
     * @param pair pair
     * @param sink sink
     * @param <S>  source
     * @param <T>  sink
     * @return pair
     */
    public static <S, T> Pair<S, T> of(@NonNull Pair<S, T> pair, T sink) {
        pair.sink = sink;
        return pair;
    }

    /**
     * modify pair source
     *
     * @param source source
     * @param pair   pair
     * @param <S>    source
     * @param <T>    sink
     * @return pair
     */
    public static <S, T> Pair<S, T> of(S source, @NonNull Pair<S, T> pair) {
        pair.source = source;
        return pair;
    }

    /**
     * @return source->sink
     */
    @Override
    public String toString() {
        return String.format("%s->%s", source, sink);
    }
}
