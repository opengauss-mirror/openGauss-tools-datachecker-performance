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
        dataBaseMySql.put(DataBaseMeta.TABLE, DataBaseMySql.TABLE_METADATA_SQL);
        dataBaseMySql.put(DataBaseMeta.COLUMN, DataBaseMySql.TABLES_COLUMN_META_DATA_SQL);
        dataBaseMySql.put(DataBaseMeta.HEALTH, DataBaseMySql.HEALTH_SQL);
        DATABASE_META_MAPPER.put(DataBaseType.MS, dataBaseMySql);
        DATABASE_TABLE_META_MAPPER.put(DataBaseType.MS, DataBaseMySql.ONE_TABLE_METADATA_SQL);

        Map<DataBaseMeta, String> dataBaseOpenGauss = new HashMap<>();
        dataBaseOpenGauss.put(DataBaseMeta.TABLE, DataBaseOpenGauss.TABLE_METADATA_SQL);
        dataBaseOpenGauss.put(DataBaseMeta.COLUMN, DataBaseOpenGauss.TABLES_COLUMN_META_DATA_SQL);
        dataBaseOpenGauss.put(DataBaseMeta.HEALTH, DataBaseOpenGauss.HEALTH_SQL);
        DATABASE_META_MAPPER.put(DataBaseType.OG, dataBaseOpenGauss);
        DATABASE_TABLE_META_MAPPER.put(DataBaseType.MS, DataBaseOpenGauss.ONE_TABLE_METADATA_SQL);

        Map<DataBaseMeta, String> databaseO = new HashMap<>();
        databaseO.put(DataBaseMeta.TABLE, DataBaseO.TABLE_METADATA_SQL);
        databaseO.put(DataBaseMeta.COLUMN, DataBaseO.TABLES_COLUMN_META_DATA_SQL);
        databaseO.put(DataBaseMeta.HEALTH, DataBaseO.HEALTH_SQL);
        DATABASE_META_MAPPER.put(DataBaseType.O, databaseO);
    }

    /**
     * build sql of query table row count
     *
     * @return table row count sql
     */
    public static String getTableCount() {
        return "select count(1) rowCount from %s.%s";
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

    public static String getOneTableMetaSql(DataBaseType databaseType) {
        Assert.isTrue(DATABASE_TABLE_META_MAPPER.containsKey(databaseType), "Database type mismatch");
        return DATABASE_TABLE_META_MAPPER.get(databaseType);
    }

    interface DataBaseMySql {
        /**
         * Health check SQL
         */
        String HEALTH_SQL = "select table_name from information_schema.tables  WHERE table_schema=? limit 1";

        /**
         * Table metadata query SQL
         */
        String TABLE_METADATA_SQL = "SELECT info.table_name tableName,info.table_rows tableRows ,info.avg_row_length avgRowLength,pk.id rowId FROM "
            + " (SELECT distinct SHA2(TABLE_NAME,224) id, TABLE_NAME FROM information_schema.KEY_COLUMN_USAGE  WHERE "
            + " TABLE_SCHEMA=:databaseSchema AND CONSTRAINT_NAME='PRIMARY' "
            + " ) pk LEFT JOIN ( select SHA2(TABLE_NAME,224) id, TABLE_NAME,TABLE_ROWS,avg_row_length from  information_schema.tables  "
            + " WHERE  table_schema=:databaseSchema ) info ON pk.id=info.id AND pk.TABLE_NAME=info.TABLE_NAME "
            + " ORDER BY info.table_rows ASC ";

        String ONE_TABLE_METADATA_SQL = "select info.table_name tableName, info.table_rows tableRows "
            + "from information_schema.tables info where info.table_schema=:databaseSchema and info.table_name=:tableName";
        /**
         * column metadata query SQL
         */
        String TABLES_COLUMN_META_DATA_SQL = "select table_name tableName ,column_name columnName,"
            + " ordinal_position ordinalPosition, data_type dataType, column_type columnType,column_key columnKey"
            + " from information_schema.columns"
            + " where table_schema=:databaseSchema and table_name in (:tableNames)";
    }

    interface DataBaseOpenGauss {
        /**
         * Health check SQL
         */
        String HEALTH_SQL = "select table_name from information_schema.tables  WHERE table_schema=? limit 1";

        /**
         * Table metadata query SQL
         */
        String TABLE_METADATA_SQL = "select c.relname tableName,c.reltuples tableRows, "
            + " case when c.reltuples>0 then pg_table_size(c.oid)/c.reltuples else 0 end as avgRowLength"
            + " from pg_class c "
            + "LEFT JOIN pg_namespace n on n.oid = c.relnamespace left join pg_index b on c.oid=b.indrelid "
            + "where n.nspname=:databaseSchema and b.indisprimary='t' order by c.reltuples asc;";

        String ONE_TABLE_METADATA_SQL = "select c.relname tableName,c.reltuples tableRows from pg_class c "
            + "LEFT JOIN pg_namespace n on n.oid = c.relnamespace left join pg_index b on c.oid=b.indrelid "
            + "where n.nspname=:databaseSchema and b.indisprimary='t' and c.relname=:tableName; ";
        /**
         * column metadata query SQL
         */
        String TABLES_COLUMN_META_DATA_SQL = "SELECT c.relname tableName ,a.attname columnName ,"
            + " a.attnum ordinalPosition,(CASE WHEN (t.typtype = 'd'::\"char\") "
            + " THEN CASE WHEN ((bt.typelem <> (0)::oid) AND (bt.typlen = (-1))) THEN 'ARRAY'::text "
            + "WHEN (nbt.nspname = 'pg_catalog'::name) THEN format_type(t.typbasetype, NULL::integer) "
            + "ELSE 'USER-DEFINED'::text END ELSE CASE WHEN ((t.typelem <> (0)::oid) AND (t.typlen = (-1))) "
            + "THEN 'ARRAY'::text WHEN (nt.nspname = 'pg_catalog'::name) THEN format_type(a.atttypid, NULL::integer) "
            + "ELSE 'USER-DEFINED'::text END END)::information_schema.character_data AS dataType ,"
            + " t.typname columnType, (case when co.contype='p'::\"char\" then 'PRI' end ) columnKey"
            + " FROM ((pg_attribute a JOIN (pg_class c JOIN pg_namespace nc ON c.relnamespace = nc.oid ) ON a.attrelid = c.oid ) "
            + "JOIN (pg_type t JOIN pg_namespace nt ON t.typnamespace = nt.oid) ON a.atttypid = t.oid) "
            + "LEFT JOIN (pg_type bt JOIN pg_namespace nbt ON bt.typnamespace = nbt.oid) "
            + "ON (t.typtype = 'd'::\"char\" AND t.typbasetype = bt.oid) "
            + "left join pg_constraint co on c.oid = co.conrelid and a.attnum = any (array[co.conkey]) "
            + "WHERE a.attnum > 0 AND (NOT a.attisdropped)  "
            + "AND (c.relkind = ANY (ARRAY['r'::\"char\", 'm'::\"char\", 'v'::\"char\", 'f'::\"char\"]))"
            + " and  nc.nspname=:databaseSchema and c.relname in (:tableNames) and c.relhaspkey=true;";
    }

    interface DataBaseO {
        /**
         * Health check SQL
         */
        String HEALTH_SQL = "";

        /**
         * Table metadata query SQL
         */
        String TABLE_METADATA_SQL = "";

        /**
         * column metadata query SQL
         */
        String TABLES_COLUMN_META_DATA_SQL = "";
    }
}
