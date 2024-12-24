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

import com.alibaba.druid.pool.DruidDataSource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.common.DataAccessParam;
import org.opengauss.datachecker.common.entry.common.PointPair;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.SqlUtil;
import org.opengauss.datachecker.extract.data.access.DataAccessService;
import org.opengauss.datachecker.extract.resource.ConnectionMgr;
import org.opengauss.datachecker.extract.util.MetaDataUtil;
import org.springframework.util.StopWatch;

import java.sql.Connection;
import java.util.ArrayList;
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
    private static final Logger log = LogUtils.getLogger(CheckPoint.class);

    private final DataAccessService dataAccessService;
    private DruidDataSource dataSource;

    /**
     * check point depends on JDBC DataSource
     *
     * @param dataAccessService dataAccessService
     */
    public CheckPoint(DataAccessService dataAccessService) {
        this.dataSource = (DruidDataSource) dataAccessService.getDataSource();
        this.dataAccessService = dataAccessService;
    }

    /**
     * init table CheckPoint List
     *
     * @param tableMetadata tableMetadata
     * @param slice slice
     * @return check point
     */
    public List<PointPair> initCheckPointList(TableMetadata tableMetadata, int slice) {
        if (slice <= 0) {
            return new LinkedList<>();
        }
        String pkName = getPkName(tableMetadata);
        String schema = tableMetadata.getSchema();
        String tableName = tableMetadata.getTableName();
        StopWatch stopWatch = new StopWatch("table check point " + tableName);
        stopWatch.start();
        DataBaseType dataBaseType = ConfigCache.getValue(ConfigConstants.DATA_BASE_TYPE, DataBaseType.class);
        DataAccessParam param = new DataAccessParam().setSchema(SqlUtil.escape(schema, dataBaseType))
            .setName(SqlUtil.escape(tableName, dataBaseType))
            .setColName(SqlUtil.escape(pkName, dataBaseType));
        Connection connection = getConnection();
        param.setOffset(slice);
        Object maxPoint = dataAccessService.max(connection, param);
        List<Object> checkPointList = dataAccessService.queryPointList(connection, param);
        checkPointList.add(maxPoint);
        checkPointList = checkPointList.stream().distinct().collect(Collectors.toList());
        stopWatch.stop();
        LogUtils.info(log, "init check-point-list table [{}]:[{}] ", tableName, stopWatch.shortSummary());
        ConnectionMgr.close(connection);
        return checkPointList.stream().map(o -> new PointPair(o, 0)).collect(Collectors.toList());
    }

    private Connection getConnection() {
        return ConnectionMgr.getConnection();
    }

    private void addCheckList(List<Object> checkList, Object value) {
        if (Objects.nonNull(value)) {
            checkList.add(value);
        }
    }

    public boolean checkPkNumber(TableMetadata tableMetadata) {
        return MetaDataUtil.isDigitPrimaryKey(tableMetadata.getSliceColumn());
    }

    private String getPkName(TableMetadata tableMetadata) {
        return tableMetadata.getSliceColumn().getColumnName();
    }

    public Long[][] translateBetween(List<Object> checkPointList) {
        Long[][] between = new Long[checkPointList.size() - 1][2];
        for (int i = 0; i < between.length; i++) {
            String value = (String) checkPointList.get(i);
            String value2 = (String) checkPointList.get(i + 1);
            between[i][0] = Objects.isNull(value) ? null : Long.parseLong(value);
            between[i][1] = Objects.isNull(value2) ? null : Long.parseLong(value2);
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
        Connection connection = ConnectionMgr.getConnection();
        String maxId = dataAccessService.max(connection, param);
        ConnectionMgr.close(connection, null, null);
        return Long.parseLong(maxId);
    }

    /**
     * 初始化联合主键表检查点
     *
     * @param tableMetadata tableMetadata
     * @return 检查点列表
     */
    public List<PointPair> initUnionPrimaryCheckPointList(TableMetadata tableMetadata) {
        List<PointPair> checkPointList = new ArrayList<>();
        List<ColumnsMetaData> primaryList = tableMetadata.getPrimaryMetas();
        for (ColumnsMetaData unionKey : primaryList) {
            List<PointPair> tmp = queryUnionKeyList(unionKey);
            if (CollectionUtils.isEmpty(tmp)) {
                tableMetadata.setSliceColumn(unionKey);
                break;
            }
            if (CollectionUtils.isEmpty(checkPointList)) {
                checkPointList = tmp;
                tableMetadata.setSliceColumn(unionKey);
            } else {
                if (checkPointList.size() > tmp.size()) {
                    checkPointList = tmp;
                    tableMetadata.setSliceColumn(unionKey);
                }
            }
        }
        return checkPointList;
    }

    private List<PointPair> queryUnionKeyList(ColumnsMetaData unionKey) {
        String colName = unionKey.getColumnName();
        String schema = unionKey.getSchema();
        String tableName = unionKey.getTableName();
        DataBaseType dataBaseType = ConfigCache.getValue(ConfigConstants.DATA_BASE_TYPE, DataBaseType.class);
        DataAccessParam param = new DataAccessParam().setSchema(SqlUtil.escape(schema, dataBaseType))
            .setName(SqlUtil.escape(tableName, dataBaseType))
            .setColName(SqlUtil.escape(colName, dataBaseType));
        try (Connection connection = getConnection()) {
            return dataAccessService.queryUnionFirstPrimaryCheckPointList(connection, param);
        } catch (Exception e) {
            log.error("{}query union primary check point list error {}", ErrorCode.BUILD_SLICE_POINT, e.getMessage());
        }
        return new LinkedList<>();
    }

    /**
     * 获取分片列名
     *
     * @param tableMetadata 表元数据
     * @return 分片列名
     */
    public String getSliceColumnName(TableMetadata tableMetadata) {
        ColumnsMetaData sliceColumn = tableMetadata.getSliceColumn();
        if (Objects.nonNull(sliceColumn)) {
            return sliceColumn.getColumnName();
        } else {
            return "";
        }
    }
}
