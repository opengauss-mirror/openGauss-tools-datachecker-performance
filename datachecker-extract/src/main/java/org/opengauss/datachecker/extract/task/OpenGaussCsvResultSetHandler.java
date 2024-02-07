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

import org.springframework.lang.NonNull;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Objects;

/**
 * OpenGaussCsvResultSetHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/19
 * @since ：11
 */
public class OpenGaussCsvResultSetHandler extends OpenGaussResultSetHandler {

    @Override
    protected String numericFloatNumberToString(@NonNull ResultSet resultSet, String columnLabel) throws SQLException {
        String floatValue = resultSet.getString(columnLabel);
        if (resultSet.wasNull()) {
            return NULL;
        }
        if (isScientificNotation(floatValue)) {
            return new BigDecimal(floatValue).toPlainString();
        }
        return floatValue;
    }

    @Override
    protected String bitToString(ResultSet rs, String columnLabel) throws SQLException {
        return rs.getString(columnLabel);
    }

    @Override
    protected String binaryToString(ResultSet rs, String columnLabel) throws SQLException {
        String binary = rs.getString(columnLabel);
        return rs.wasNull() ? NULL : Objects.isNull(binary) ? NULL : binary.substring(2)
                                                                           .toUpperCase(Locale.ENGLISH);
    }
}
