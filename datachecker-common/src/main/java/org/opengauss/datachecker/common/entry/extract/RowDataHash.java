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

package org.opengauss.datachecker.common.entry.extract;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Objects;

/**
 * RowDataHash
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/1
 * @since ：11
 */
@Data
@Accessors(chain = true)
public class RowDataHash {
    /**
     * <pre>
     * If the primary key is a numeric type, it will be converted to a string.
     * If the table primary key is a joint primary key, the current attribute will be a table primary key,
     * and the corresponding values of the joint fields will be spliced. String splicing will be underlined
     * </pre>
     */
    private String key;

    private String sliceKey;
    /**
     * CSV scene for locating data in CSV files
     */
    private int idx;

    /**
     * Hash value of the corresponding value of the primary key
     */
    private long kHash;
    /**
     * Total hash value of the current record
     */
    private long vHash;

    /**
     * slice no
     */
    private int sNo;

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RowDataHash)) {
            return false;
        }
        RowDataHash that = (RowDataHash) o;
        return kHash == that.kHash && vHash == that.vHash && sNo == that.sNo && Objects.equals(getKey(), that.getKey())
                && Objects.equals(getSliceKey(), that.getSliceKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getKey(), kHash, vHash, sNo, sliceKey);
    }
}