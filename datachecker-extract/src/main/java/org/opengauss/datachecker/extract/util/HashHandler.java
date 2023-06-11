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
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

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
     * @param columns         List of field names
     * @return Hash calculation result corresponding to the current row
     */
    public long xx3Hash(Map<String, String> columnsValueMap, List<String> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return 0L;
        }
        String colValue =
            columnsValueMap.entrySet().stream().filter(entry -> columns.contains(entry.getKey())).map(Entry::getValue)
                           .collect(Collectors.joining());
        return XX_3_HASH.hashChars(colValue);
    }

    /**
     * column hash result
     *
     * @param columnsValueMap columns value
     * @param columns         column names
     * @return column hash result
     */
    public String value(Map<String, String> columnsValueMap, List<String> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return "";
        }
        List<String> values = new ArrayList<>();
        columns.forEach(column -> {
            if (columnsValueMap.containsKey(column)) {
                values.add(columnsValueMap.get(column));
            }
        });
        return values.stream().map(String::valueOf).collect(Collectors.joining(PRIMARY_DELIMITER));
    }
}
