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

package org.opengauss.datachecker.extract.dao;

import org.opengauss.datachecker.common.entry.enums.DataBaseMeta;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;

/**
 * MetaSqlMapper
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/24
 * @since ：11
 */
public class MetaSqlMapper {
    private static final Map<DataBaseType, Map<DataBaseMeta, String>> DATABASE_META_MAPPER = new HashMap<>();
    private static final Map<DataBaseType, String> DATABASE_TABLE_META_MAPPER = new HashMap<>();

    static {
        Map<DataBaseMeta, String> dataBaseMySql = new HashMap<>();
        dataBaseMySql.put(DataBaseMeta.MAX_ID_COUNT, DataBaseMySql.TABLE_MAX_COUNTS);
        DATABASE_META_MAPPER.put(DataBaseType.MS, dataBaseMySql);

        Map<DataBaseMeta, String> dataBaseOpenGauss = new HashMap<>();
        dataBaseOpenGauss.put(DataBaseMeta.MAX_ID_COUNT, DataBaseOpenGauss.TABLE_MAX_COUNTS);
        DATABASE_META_MAPPER.put(DataBaseType.OG, dataBaseOpenGauss);
    }

    /**
     * Return the corresponding metadata execution statement according to the database type
     * and the metadata query type currently to be executed
     *
     * @param databaseType database type
     * @param databaseMeta 数据库元数据
     * @return execute sql
     */
    public static String getMetaSql(DataBaseType databaseType, DataBaseMeta databaseMeta) {
        Assert.isTrue(DATABASE_META_MAPPER.containsKey(databaseType), "Database type mismatch");
        return DATABASE_META_MAPPER.get(databaseType).get(databaseMeta);
    }

    interface DataBaseMySql {
        String TABLE_MAX_COUNTS = "select (case when max(%s) is null then 1 else max(%s) end) as count from %s.%s";
    }

    interface DataBaseOpenGauss {
        String TABLE_MAX_COUNTS = "select (case when max(%s) is null then 1 else max(%s) end) as count from %s.%s";
    }
}
