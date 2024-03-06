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

package org.opengauss.datachecker.extract.data.access;

import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.common.DataAccessParam;
import org.opengauss.datachecker.common.entry.enums.OgCompatibility;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.PrimaryColumnBean;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.data.mapper.OpgsMetaDataMapper;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Objects;

/**
 * OpgsDataAccessService
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/10
 * @since ：11
 */
public class OpgsDataAccessService extends AbstractDataAccessService {
    private OpgsMetaDataMapper opgsMetaDataMapper;

    public OpgsDataAccessService(OpgsMetaDataMapper opgsMetaDataMapper) {
        this.opgsMetaDataMapper = opgsMetaDataMapper;
    }

    @Override
    @PostConstruct
    public boolean isOgCompatibilityB() {
        isOgCompatibilityB = Objects.equals(OgCompatibility.B, opgsMetaDataMapper.sqlCompatibility());
        ConfigCache.put(ConfigConstants.OG_COMPATIBILITY_B, isOgCompatibilityB);
        return isOgCompatibilityB;
    }

    @Override
    public String sqlMode() {
        return isOgCompatibilityB() ? opgsMetaDataMapper.dolphinSqlMode() : opgsMetaDataMapper.sqlMode();
    }

    @Override
    public boolean health() {
        return opgsMetaDataMapper.health();
    }

    @Override
    public List<String> dasQueryTableNameList() {
        String schema = properties.getSchema();
        String sql = "select c.relname tableName from pg_class c  LEFT JOIN pg_namespace n on n.oid = c.relnamespace "
            + " where n.nspname='" + schema + "' and c.relkind ='r';";
        return adasQueryTableNameList(sql);
    }

    @Override
    public List<PrimaryColumnBean> queryTablePrimaryColumns() {
        String schema = properties.getSchema();
        String sql = "select c.relname tableName,ns.nspname,ns.oid,a.attname columnName from pg_class c "
            + "left join pg_namespace ns on c.relnamespace=ns.oid "
            + "left join pg_attribute a on c.oid=a.attrelid and a.attnum>0 and not a.attisdropped "
            + "inner join pg_constraint cs on a.attrelid=cs.conrelid and a.attnum=any(cs.conkey) "
            + "where ns.nspname='" + schema + "' and cs.contype='p';";
        return adasQueryTablePrimaryColumns(sql);
    }

    @Override
    public List<PrimaryColumnBean> queryTablePrimaryColumns(String tableName) {
        String schema = properties.getSchema();
        String sql = "select c.relname tableName,ns.nspname,ns.oid,a.attname columnName from pg_class c "
            + "left join pg_namespace ns on c.relnamespace=ns.oid "
            + "left join pg_attribute a on c.oid=a.attrelid and a.attnum>0 and not a.attisdropped "
            + "inner join pg_constraint cs on a.attrelid=cs.conrelid and a.attnum=any(cs.conkey) "
            + "where ns.nspname='" + schema + "' and c.relname='" + tableName + "' and cs.contype='p';";
        return adasQueryTablePrimaryColumns(sql);
    }

    @Override
    public List<ColumnsMetaData> queryTableColumnsMetaData(String tableName) {
        return opgsMetaDataMapper.queryTableColumnsMetaData(properties.getSchema(), tableName);
    }

    @Override
    public TableMetadata queryTableMetadata(String tableName) {
        return wrapperTableMetadata(opgsMetaDataMapper.queryTableMetadata(properties.getSchema(), tableName));
    }

    @Override
    public List<TableMetadata> dasQueryTableMetadataList() {
        String sql = " select n.nspname tableSchema, c.relname tableName,c.reltuples tableRows, "
            + "case when c.reltuples>0 then pg_table_size(c.oid)/c.reltuples else 0 end as avgRowLength "
            + "from pg_class c LEFT JOIN pg_namespace n on n.oid = c.relnamespace " + "where n.nspname='"
            + properties.getSchema() + "' and c.relkind ='r';";
        return wrapperTableMetadata(adasQueryTableMetadataList(sql));
    }

    @Override
    public long rowCount(String tableName) {
        return opgsMetaDataMapper.rowCount(properties.getSchema(), tableName);
    }

    @Override
    public String min(DataAccessParam param) {
        return opgsMetaDataMapper.min(param);
    }

    @Override
    public String max(DataAccessParam param) {
        return opgsMetaDataMapper.max(param);
    }

    @Override
    public String next(DataAccessParam param) {
        return opgsMetaDataMapper.next(param);
    }

    @Override
    public List<Object> queryPointList(DataAccessParam param) {
        String sql = "select s.%s from ( select row_number() over(order by r.%s  asc) as rn,r.%s  from %s.%s r) s"
            + "  where mod(s.rn, %s ) = 0;";
        sql = String.format(sql, param.getColName(), param.getColName(), param.getColName(), param.getSchema(),
            param.getName(), param.getOffset());
        return adasQueryPointList(sql);
    }

    @Override
    public boolean dasCheckDatabaseNotEmpty() {
        return opgsMetaDataMapper.checkDatabaseNotEmpty(properties.getSchema());
    }
}
