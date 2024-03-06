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

package org.opengauss.datachecker.extract.task.functional;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * SimpleTypeHandler
 *
 * @author ：wangchao
 * @date ：Created in 2024/2/26
 * @since ：11
 */
@FunctionalInterface
public interface SimpleTypeHandler {
    /**
     * result convert to string
     *
     * @param resultSet   resultSet
     * @param columnLabel columnLabel
     * @return result result
     * @throws SQLException SQLException
     */
    String convert(ResultSet resultSet, String columnLabel) throws SQLException;
}
