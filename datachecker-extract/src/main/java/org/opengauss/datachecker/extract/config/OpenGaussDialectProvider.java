/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
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

package org.opengauss.datachecker.extract.config;

import org.springframework.data.jdbc.core.dialect.DialectResolver;
import org.springframework.data.jdbc.core.dialect.JdbcPostgresDialect;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcOperations;

import java.sql.DatabaseMetaData;
import java.util.Optional;

/**
 * JDBC dialect provider that maps openGauss family connections (openGauss, oGRAC) to the Postgres dialect.
 *
 * @author : xujintao
 * @date : Created in 2026/9/7
 * @since : 11
 */
public class OpenGaussDialectProvider implements DialectResolver.JdbcDialectProvider {
    @Override
    public Optional<Dialect> getDialect(JdbcOperations operations) {
        return operations.execute((ConnectionCallback<Optional<Dialect>>) connection -> {
            DatabaseMetaData metaData = connection.getMetaData();
            if ("openGauss".equalsIgnoreCase(metaData.getDatabaseProductName())) {
                return Optional.of(JdbcPostgresDialect.INSTANCE);
            }
            String url = metaData.getURL();
            if (url != null && url.startsWith("jdbc:oGRAC:")) {
                return Optional.of(JdbcPostgresDialect.INSTANCE);
            }
            return Optional.empty();
        });
    }
}
