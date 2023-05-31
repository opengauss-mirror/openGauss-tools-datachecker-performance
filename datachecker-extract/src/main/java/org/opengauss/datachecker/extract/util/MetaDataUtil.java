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

import org.apache.commons.collections4.CollectionUtils;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * MetaDataUtil
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/15
 * @since ：11
 */
public class MetaDataUtil {
    /**
     * getTableColumns
     *
     * @param tableMetadata tableMetadata
     * @return table Columns
     */
    public static List<String> getTableColumns(TableMetadata tableMetadata) {
        if (Objects.isNull(tableMetadata)) {
            return emptyList();
        }
        List<ColumnsMetaData> columnsMetas = tableMetadata.getColumnsMetas();
        return getTableColumns(columnsMetas);
    }

    /**
     * getTablePrimaryColumns
     *
     * @param tableMetadata tableMetadata
     * @return table Columns
     */
    public static List<String> getTablePrimaryColumns(TableMetadata tableMetadata) {
        if (Objects.isNull(tableMetadata)) {
            return emptyList();
        }
        List<ColumnsMetaData> primaryMetas = tableMetadata.getPrimaryMetas();
        return getTableColumns(primaryMetas);
    }

    private static ArrayList<String> emptyList() {
        return new ArrayList<>(0);
    }

    private static List<String> getTableColumns(List<ColumnsMetaData> columnsMetas) {
        if (Objects.isNull(columnsMetas)) {
            return emptyList();
        }
        return columnsMetas.stream().sorted(Comparator.comparing(ColumnsMetaData::getOrdinalPosition))
                           .map(ColumnsMetaData::getColumnName).collect(Collectors.toUnmodifiableList());
    }

    /**
     * hasNoPrimary
     *
     * @param tableMetadata tableMetadata
     * @return true | false
     */
    public static boolean hasNoPrimary(TableMetadata tableMetadata) {
        return CollectionUtils.isEmpty(tableMetadata.getPrimaryMetas());
    }
}
