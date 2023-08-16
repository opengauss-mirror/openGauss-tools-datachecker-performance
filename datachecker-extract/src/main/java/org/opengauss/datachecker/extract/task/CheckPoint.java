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
import org.opengauss.datachecker.common.entry.common.DataAccessParam;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.data.access.DataAccessService;

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
    private static final List<String> numberDataTypes =
        List.of("integer", "int", "long", "smallint", "mediumint", "bigint");
    private static final List<String> dataTypes =
        List.of("integer", "int", "long", "smallint", "mediumint", "bigint", "character", "char", "varchar",
            "character varying");
    private final DataAccessService dataAccessService;

    /**
     * check point depends on JDBC DataSource
     *
     * @param dataAccessService
     */
    public CheckPoint(DataAccessService dataAccessService) {
        this.dataAccessService = dataAccessService;
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

        DataAccessParam param = new DataAccessParam().setSchema(schema).setName(tableName).setColName(pkName);
        String checkPoint = dataAccessService.min(param);
        param.setOffset(slice);
        String preValue = checkPoint;
        List<Object> checkList = new LinkedList<>();
        checkList.add(preValue);
        while (preValue != null) {
            param.setPreValue(preValue);
            preValue = dataAccessService.next(param);
            if (preValue != null) {
                checkList.add(preValue);
            }
        }
        Object maxPoint = dataAccessService.max(param);
        checkList.add(maxPoint);
        log.info("table [{}] check-point-list : {} ", tableName, checkList);
        return checkList;
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
            String value = (String) checkPointList.get(i);
            String value2 = (String) checkPointList.get(i + 1);
            between[i][0] = Long.parseLong(value);
            between[i][1] = Long.parseLong(value2);
        }
        return between;
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
        return dataAccessService.rowCount(tableMetadata.getTableName());
    }

    public long queryMaxIdOfAutoIncrementTable(TableMetadata tableMetadata) {
        DataAccessParam param = new DataAccessParam();
        param.setSchema(tableMetadata.getSchema()).setName(tableMetadata.getTableName())
             .setColName(getPkName(tableMetadata));
        String maxId = dataAccessService.max(param);
        return Long.parseLong(maxId);
    }

    public boolean checkValidPrimaryKey(TableMetadata tableMetadata) {
        String columnType = tableMetadata.getPrimaryMetas().get(0).getDataType();
        return !dataTypes.contains(columnType);
    }
}
