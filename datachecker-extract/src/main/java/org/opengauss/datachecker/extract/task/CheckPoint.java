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

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.common.DataAccessParam;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.data.access.DataAccessService;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * CheckPoint
 *
 * @author ：wangchao
 * @date ：Created in 2023/6/19
 * @since ：11
 */
public class CheckPoint {
    private static final Logger log = LogUtils.getLogger();
    private static final List<String> numberDataTypes =
        List.of("integer", "int", "uint1", "uint2", "uint4", "uint8", "long", "decimal", "numeric", "smallint",
            "mediumint", "bigint");
    private static final List<String> dataTypes =
        List.of("integer", "int", "uint1", "uint2", "uint4", "uint8", "long", "decimal", "numeric", "smallint",
            "mediumint", "bigint", "character", "char", "varchar", "character varying");
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

        DataAccessParam param = new DataAccessParam().setSchema(schema)
                                                     .setName(tableName)
                                                     .setColName(pkName);
        String minCheckPoint = dataAccessService.min(param);
        param.setOffset(slice);
        Object maxPoint = dataAccessService.max(param);
        List<Object> checkPointList = dataAccessService.queryPointList(param);
        checkPointList.add(minCheckPoint);
        checkPointList.add(maxPoint);
        checkPointList = checkPointList.stream()
                                       .distinct()
                                       .collect(Collectors.toList());
        log.debug("table [{}] check-point-list : {} ", tableName, checkPointList);
        return checkPointList;
    }

    private void addCheckList(List<Object> checkList, Object value) {
        if (Objects.nonNull(value)) {
            checkList.add(value);
        }
    }

    public boolean checkPkNumber(TableMetadata tableMetadata) {
        String dataType = tableMetadata.getPrimaryMetas()
                                       .get(0)
                                       .getDataType();
        return numberDataTypes.contains(dataType);
    }

    private String getPkName(TableMetadata tableMetadata) {
        return tableMetadata.getPrimaryMetas()
                            .get(0)
                            .getColumnName();
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
        param.setSchema(tableMetadata.getSchema())
             .setName(tableMetadata.getTableName())
             .setColName(getPkName(tableMetadata));
        String maxId = dataAccessService.max(param);
        return Long.parseLong(maxId);
    }

    public boolean checkInvalidPrimaryKey(TableMetadata tableMetadata) {
        String columnType = tableMetadata.getPrimaryMetas()
                                         .get(0)
                                         .getDataType();
        return !dataTypes.contains(columnType);
    }
}
