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

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.check.Difference;
import org.opengauss.datachecker.common.entry.enums.DataBaseMeta;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.dao.MetaSqlMapper;
import org.opengauss.datachecker.extract.task.ResultSetHandlerFactory;
import org.opengauss.datachecker.extract.task.ResultSetHashHandler;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/**
 * AbstractDataAccessService
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/10
 * @since ：11
 */
public abstract class AbstractDataAccessService implements DataAccessService {
    private static final Logger log = LogUtils.getLogger();
    protected boolean isOgCompatibilityB = false;
    @Resource
    protected JdbcTemplate jdbcTemplate;
    @Resource
    protected ExtractProperties properties;
    private final ResultSetHashHandler resultSetHashHandler = new ResultSetHashHandler();
    private final ResultSetHandlerFactory resultSetFactory = new ResultSetHandlerFactory();

    @Override
    public <T> List<T> query(String sql, Map<String, Object> param, RowMapper<T> rowMapper) {
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(jdbcTemplate);
        return jdbc.query(sql, param, rowMapper);
    }

    @Override
    public DataSource getDataSource() {
        return jdbcTemplate.getDataSource();
    }

    /**
     * wrapper table metadata of endpoint and databaseType
     *
     * @param tableMetadata tableMetadata
     * @return tableMetadata
     */
    protected TableMetadata wrapperTableMetadata(TableMetadata tableMetadata) {
        if (tableMetadata == null) {
            return null;
        }
        return tableMetadata.setDataBaseType(properties.getDatabaseType())
                            .setEndpoint(properties.getEndpoint())
                            .setOgCompatibilityB(isOgCompatibilityB);
    }

    /**
     * jdbc mode does not use it
     *
     * @param table          table
     * @param fileName       fileName
     * @param differenceList differenceList
     * @return
     */
    @Override
    public List<Map<String, String>> query(String table, String fileName, List<Difference> differenceList) {
        return null;
    }

    /**
     * wrapper table metadata of endpoint and databaseType
     *
     * @param list list of TableMetadata
     * @return tableMetadata
     */
    protected List<TableMetadata> wrapperTableMetadata(List<TableMetadata> list) {
        list.stream()
            .forEach(meta -> {
                meta.setDataBaseType(properties.getDatabaseType())
                    .setEndpoint(properties.getEndpoint())
                    .setOgCompatibilityB(isOgCompatibilityB);
            });
        return list;
    }

    private <T> List<T> queryByCondition(String sql, Map<String, Object> paramMap, RowMapper<T> rowMapper) {
        LocalDateTime start = LocalDateTime.now();
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(jdbcTemplate);
        List<T> results = jdbc.query(sql, paramMap, rowMapper);
        log.debug("query sql:<{}> size:{} cost:{}", sql, results.size(), Duration.between(start, LocalDateTime.now())
                                                                                 .toSeconds());
        return results;
    }

    protected String getSql(DataBaseMeta type) {
        return MetaSqlMapper.getMetaSql(properties.getDatabaseType(), type);
    }
}
