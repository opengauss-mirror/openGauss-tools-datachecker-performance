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

import com.mysql.cj.MysqlType;
import org.opengauss.datachecker.common.util.HexUtil;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MysqlResultSetHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/19
 * @since ：11
 */
public class MysqlResultSetHandler extends ResultSetHandler {
    private final Map<MysqlType, TypeHandler> typeHandlers = new ConcurrentHashMap<>();

    {
        TypeHandler binaryToString = (rs, columnLabel) -> bytesToString(rs.getBytes(columnLabel));
        TypeHandler blobToString = (rs, columnLabel) -> HexUtil.byteToHexTrim(rs.getBytes(columnLabel));
        TypeHandler numericToString = (rs, columnLabel) -> floatingPointNumberToString(rs, columnLabel);
        TypeHandler charToString = (rs, columnLabel) -> fixedLenCharToString(rs, columnLabel);

        typeHandlers.put(MysqlType.FLOAT_UNSIGNED, numericToString);
        typeHandlers.put(MysqlType.FLOAT, numericToString);
        typeHandlers.put(MysqlType.DOUBLE, numericToString);
        typeHandlers.put(MysqlType.DOUBLE_UNSIGNED, numericToString);
        typeHandlers.put(MysqlType.DECIMAL, numericToString);
        typeHandlers.put(MysqlType.DECIMAL_UNSIGNED, numericToString);
        // byte binary blob
        typeHandlers.put(MysqlType.BINARY, binaryToString);
        typeHandlers.put(MysqlType.VARBINARY, binaryToString);

        typeHandlers.put(MysqlType.CHAR, charToString);

        typeHandlers.put(MysqlType.BLOB, blobToString);
        typeHandlers.put(MysqlType.LONGBLOB, blobToString);
        typeHandlers.put(MysqlType.MEDIUMBLOB, blobToString);
        typeHandlers.put(MysqlType.TINYBLOB, blobToString);

        // date time timestamp
        typeHandlers.put(MysqlType.DATE, this::getDateFormat);
        typeHandlers.put(MysqlType.DATETIME, this::getTimestampFormat);
        typeHandlers.put(MysqlType.TIME, this::getTimeFormat);
        typeHandlers.put(MysqlType.TIMESTAMP, this::getTimestampFormat);
        typeHandlers.put(MysqlType.YEAR, this::getYearFormat);
    }

    private String bitToString(ResultSet resultSet, String columnLabel, int precision) throws SQLException {
        if (precision == 1) {
            return resultSet.getString(columnLabel);
        }
        return HexUtil.byteToHexTrim(resultSet.getBytes(columnLabel));
    }

    private String byteToStringTrim(byte[] bytes) {
        return HexUtil.byteToHexTrim(bytes);
    }

    @Override
    protected String convert(ResultSet resultSet, int columnIdx, ResultSetMetaData rsmd) throws SQLException {
        String columnLabel = rsmd.getColumnLabel(columnIdx);
        String columnTypeName = rsmd.getColumnTypeName(columnIdx);
        int precision = rsmd.getPrecision(columnIdx);
        final MysqlType mysqlType = MysqlType.getByName(columnTypeName);
        if (MysqlType.BIT.equals(mysqlType)) {
            return bitToString(resultSet, columnLabel, precision);
        } else if (typeHandlers.containsKey(mysqlType)) {
            return typeHandlers.get(mysqlType)
                               .convert(resultSet, columnLabel);
        } else {
            Object object = resultSet.getObject(columnLabel);
            return Objects.isNull(object) ? NULL : object.toString();
        }
    }
}