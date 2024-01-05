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

import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;

import java.util.Objects;

/**
 * ResultSetHandlerFactory
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/19
 * @since ：11
 */
public class ResultSetHandlerFactory {
    /**
     * create ResultSetHandler
     *
     * @param databaseType databaseType
     * @return ResultSetHandler
     */
    public ResultSetHandler createHandler(DataBaseType databaseType) {
        Boolean supplyZero = ConfigCache.getBooleanValue(ConfigConstants.FLOATING_POINT_DATA_SUPPLY_ZERO);
        CheckMode checkMode = ConfigCache.getCheckMode();
        if (Objects.equals(databaseType, DataBaseType.MS)) {
            return new MysqlResultSetHandler();
        } else if (Objects.equals(databaseType, DataBaseType.OG)) {
            if (Objects.equals(CheckMode.CSV, checkMode)) {
                return new OpenGaussCsvResultSetHandler();
            } else {
                return new OpenGaussResultSetHandler(supplyZero);
            }
        } else if (Objects.equals(databaseType, DataBaseType.O)) {
            return new OracleResultSetHandler();
        } else {
            return null;
        }
    }
}
