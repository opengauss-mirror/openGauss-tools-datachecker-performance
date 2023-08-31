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

package org.opengauss.datachecker.common.util;

import org.springframework.util.CollectionUtils;

import java.util.Collection;

/**
 * CheckResultUtils
 *
 * @author ：wangchao
 * @date ：Created in 2023/9/6
 * @since ：11
 */
public class CheckResultUtils {
    public static <T> boolean isEmptyDiff(Collection<T> deletes, Collection<T> updates, Collection<T> inserts) {
        return CollectionUtils.isEmpty(deletes) && CollectionUtils.isEmpty(updates) && CollectionUtils.isEmpty(inserts);
    }

    public static <T, E> boolean isEmptyDiff(Collection<T> coll1, Collection<E> coll2) {
        return CollectionUtils.isEmpty(coll1) && CollectionUtils.isEmpty(coll2);
    }

    public static <T, E> boolean isNotEmptyDiff(Collection<T> coll1, Collection<E> coll2) {
        return !isEmptyDiff(coll1, coll2);
    }
}
