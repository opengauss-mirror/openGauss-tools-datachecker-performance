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

package org.opengauss.datachecker.extract.task;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;

import javax.sql.DataSource;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * CheckPoint
 *
 * @author ：wangchao
 * @date ：Created in 2023/6/19
 * @since ：11
 */
@Slf4j
public class CheckPoint {
    private static final String checkPointMin = "select min(%s) from %s.%s";
    private static final String checkPointMax = "select max(%s) from %s.%s";
    private static final String checkPointRowCount = "select count(%s) from %s.%s";
    private static final String nextCheckPointPrefix = "select %s from %s.%s";
    private static final String nextCheckPointCondition = " where %s >= %s order by %s asc limit %s ,1";
    private static final List<String> numberDataTypes =
        List.of("integer", "int", "long", "smallint", "mediumint", "bigint");
    private static final List<String> dataTypes =
        List.of("integer", "int", "long", "smallint", "mediumint", "bigint", "character", "char", "varchar",
            "character varying");
    private final DataSource dataSource;

    /**
     * check point depends on JDBC DataSource
     *
     * @param dataSource
     */
    public CheckPoint(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * init table CheckPoint List
     *
     * @param tableMetadata tableMetadata
     * @param slice         slice
     * @return
     */
    public List<Object> initCheckPointList(TableMetadata tableMetadata, int slice) {
        if (slice <= 0) {
            return new LinkedList<>();
        }
        String pkName = getPkName(tableMetadata);
        String schema = tableMetadata.getSchema();
        String tableName = tableMetadata.getTableName();
        Object checkPoint = getInitCheckPoint(pkName, schema, tableName);
        String nextCheckSqlPrefix = String.format(nextCheckPointPrefix, pkName, schema, tableName);
        Object preValue = checkPoint;
        List<Object> checkList = new LinkedList<>();
        checkList.add(preValue);
        while (preValue != null) {
            preValue = nextCheckPoint(nextCheckSqlPrefix, pkName, preValue, slice);
            if (preValue != null) {
                checkList.add(preValue);
            }
        }
        Object maxPoint = getMaxCheckPoint(pkName, schema, tableName);
        checkList.add(maxPoint);
        log.info("table [{}] check-point-list : {} ", tableName, checkList);
        return checkList;
    }

    private Object getInitCheckPoint(String pkName, String schema, String tableName) {
        return execute(String.format(checkPointMin, pkName, schema, tableName));
    }

    private Object getMaxCheckPoint(String pkName, String schema, String tableName) {
        return execute(String.format(checkPointMax, pkName, schema, tableName));
    }

    private Object nextCheckPoint(String nextCheckSqlPrefix, String pkName, Object preValue, int slice) {
        return execute(nextCheckSqlPrefix + String.format(nextCheckPointCondition, pkName, preValue, pkName, slice));
    }

    private Object execute(String sql) {
        Object point = null;
        try (Connection connection = dataSource.getConnection();
            PreparedStatement ps = connection.prepareStatement(sql); ResultSet resultSet = ps.executeQuery()) {
            if (resultSet.next()) {
                point = resultSet.getObject(1);
            }
        } catch (SQLException ex) {
            throw new ExtractDataAccessException("check point error : " + ex.getMessage());
        }
        return point;
    }

    public boolean checkPkNumber(TableMetadata tableMetadata) {
        String dataType = tableMetadata.getPrimaryMetas().get(0).getDataType();
        return numberDataTypes.contains(dataType);
    }

    private String getPkName(TableMetadata tableMetadata) {
        return tableMetadata.getPrimaryMetas().get(0).getColumnName();
    }

    public Long[][] translateBetween(List<Object> checkPointList) {
        Long[][] between = new Long[checkPointList.size() - 1][2];
        for (int i = 0; i < between.length; i++) {
            Object value = checkPointList.get(i);
            Object value2 = checkPointList.get(i + 1);
            between[i][0] = objectTranslateToLong(value);
            between[i][1] = objectTranslateToLong(value2);
        }
        return between;
    }

    private long objectTranslateToLong(Object value) {
        if (value instanceof Integer) {
            return ((Integer) value).longValue();
        } else if (value instanceof BigInteger) {
            return ((BigInteger) value).longValue();
        } else {
            return (Long) value;
        }
    }

    public String[][] translateBetweenString(List<Object> checkPointList) {
        String[][] between = new String[checkPointList.size() - 1][2];
        for (int i = 0; i < between.length; i++) {
            between[i][0] = (String) checkPointList.get(i);
            between[i][1] = (String) checkPointList.get(i + 1);
        }
        return between;
    }

    public long queryRowsOfAutoIncrementTable(TableMetadata tableMetadata) {
        Object rowCount = execute(String.format(checkPointRowCount, getPkName(tableMetadata), tableMetadata.getSchema(),
            tableMetadata.getTableName()));
        return objectTranslateToLong(rowCount);
    }

    public long queryMaxIdOfAutoIncrementTable(TableMetadata tableMetadata) {
        Object maxId = execute(String
            .format(checkPointMax, getPkName(tableMetadata), tableMetadata.getSchema(), tableMetadata.getTableName()));
        return objectTranslateToLong(maxId);
    }

    public boolean checkValidPrimaryKey(TableMetadata tableMetadata) {
        String columnType = tableMetadata.getPrimaryMetas().get(0).getDataType();
        return !dataTypes.contains(columnType);
    }
}
