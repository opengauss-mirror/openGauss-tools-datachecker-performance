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

package org.opengauss.datachecker.extract.util;

import net.openhft.hashing.LongHashFunction;

import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opengauss.datachecker.extract.constants.ExtConstants.PRIMARY_DELIMITER;

/**
 * The hash processor performs hash calculation on the query results.
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/15
 * @since ：11
 */
public class HashHandler {
    private static final long XX3_SEED = 199972221018L;

    /**
     * hashing algorithm
     */
    private static final LongHashFunction XX_3_HASH = LongHashFunction.xx3(XX3_SEED);

    /**
     * According to the field list set in the columns set,
     * find the corresponding value of the field in the map, and splice the found value.
     *
     * @param columnsValueMap Field corresponding query data
     * @param columns List of field names
     * @return Hash calculation result corresponding to the current row
     */
    public long xx3Hash(Map<String, String> columnsValueMap, List<String> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return 0L;
        }
        StringBuilder valueBuffer = new StringBuilder();
        for (String column : columns) {
            valueBuffer.append(columnsValueMap.getOrDefault(column, ""));
        }
        return XX_3_HASH.hashChars(valueBuffer);
    }

    /**
     * Hash calculation of a single field
     *
     * @param value field value
     * @return hash result
     */
    public long xx3Hash(String value) {
        return XX_3_HASH.hashChars(value);
    }

    /**
     * column hash result
     *
     * @param columnsValueMap columns value
     * @param columns column names
     * @return column hash result
     */
    public String value(Map<String, String> columnsValueMap, List<String> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return "";
        }
        List<String> values = new ArrayList<>();
        if (columns.size() == 1) {
            return columnsValueMap.getOrDefault(columns.get(0), "");
        } else if (columns.size() == 2) {
            return columnsValueMap.get(columns.get(0)) + PRIMARY_DELIMITER + columnsValueMap.get(columns.get(1));
        } else {
            columns.forEach(column -> {
                if (columnsValueMap.containsKey(column)) {
                    values.add(columnsValueMap.get(column));
                }
            });
            return StringUtils.join(values, PRIMARY_DELIMITER);
        }
    }
}
