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

package org.opengauss.datachecker.extract.data.mapper.handler;

import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;
import org.opengauss.datachecker.common.entry.enums.IEnum;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * EnumKeyTypeHandler
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/12
 * @since ：11
 */
public class EnumKeyTypeHandler extends BaseTypeHandler<IEnum> {
    private Class<IEnum> type;
    private final IEnum[] enums;

    public EnumKeyTypeHandler(Class<IEnum> type) {
        if (type == null) {
            throw new IllegalArgumentException("Type argument cannot be null");
        }
        this.type = type;
        this.enums = type.getEnumConstants();
        if (this.enums == null) {
            throw new IllegalArgumentException(type.getSimpleName() + " does not represent an enum type.");
        }
    }

    @Override
    public void setNonNullParameter(PreparedStatement ps, int i, IEnum parameter, JdbcType jdbcType)
        throws SQLException {
        ps.setString(i, parameter.getCode());
    }

    @Override
    public IEnum getNullableResult(ResultSet rs, String columnName) throws SQLException {
        if (rs.wasNull()) {
            return null;
        } else {
            return locateEnum(rs.getString(columnName));
        }
    }

    @Override
    public IEnum getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
        if (rs.wasNull()) {
            return null;
        } else {
            return locateEnum(rs.getString(columnIndex));
        }
    }

    @Override
    public IEnum getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
        if (cs.wasNull()) {
            return null;
        } else {
            return locateEnum(cs.getString(columnIndex));
        }
    }

    private IEnum locateEnum(String code) {
        if ("".equals(code) || code == null) {
            return null;
        }
        for (IEnum status : enums) {
            if (status.getCode().equals(code)) {
                return status;
            }
        }
        throw new IllegalArgumentException("unknown enum type : " + code + ", please checked " + type.getSimpleName());
    }
}
