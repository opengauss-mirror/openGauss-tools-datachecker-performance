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

package org.opengauss.datachecker.extract.service;

import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.opengauss.datachecker.common.entry.common.RepairEntry;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.exception.BuildRepairStatementException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.constants.ExtConstants;
import org.opengauss.datachecker.extract.data.access.DataAccessService;
import org.opengauss.datachecker.extract.dml.DeleteDmlBuilder;
import org.opengauss.datachecker.extract.dml.InsertDmlBuilder;
import org.opengauss.datachecker.extract.dml.UpdateDmlBuilder;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * BaseRepairStatement
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/25
 * @since ：11
 */
public abstract class BaseRepairStatement implements RepairStatement {
    protected static final Logger log = LogUtils.getLogger();
    @Resource
    protected MetaDataService metaDataService;
    @Resource
    protected DataAccessService dataAccessService;

    protected abstract void repairStatementLog(RepairEntry repairEntry);

    protected abstract boolean checkDiffEmpty(RepairEntry repairEntry);

    protected List<String> buildInsertDml(RepairEntry repairEntry, TableMetadata metadata,
        List<Map<String, String>> diffValues, Iterator<String> iterator) {
        List<String> resultList = new ArrayList<>();
        InsertDmlBuilder builder = new InsertDmlBuilder(DataBaseType.OG, repairEntry.isOgCompatibility());
        builder.schema(repairEntry.getSchema())
               .tableName(repairEntry.getTable())
               .columns(metadata.getColumnsMetas());
        Map<String, Map<String, String>> compositeKeyValues =
            transtlateColumnValues(diffValues, metadata.getPrimaryMetas());
        iterator.forEachRemaining(compositeKey -> {
            Map<String, String> columnValue = compositeKeyValues.get(compositeKey);
            if (MapUtils.isNotEmpty(columnValue)) {
                resultList.add(builder.columnsValue(columnValue, metadata.getColumnsMetas())
                                      .build());
            }
        });
        return resultList;
    }

    protected List<String> buildUpdateDml(RepairEntry repairEntry, TableMetadata metadata,
        List<Map<String, String>> columnValues, Iterator<String> iterator) {
        List<String> resultList = new ArrayList<>();
        Map<String, Map<String, String>> compositeKeyValues =
            transtlateColumnValues(columnValues, metadata.getPrimaryMetas());
        UpdateDmlBuilder builder =
            createUpdateDmlBuilder(repairEntry.getSchema(), repairEntry.isOgCompatibility(), metadata);
        iterator.forEachRemaining(compositeKey -> {
            Map<String, String> columnValue = compositeKeyValues.get(compositeKey);
            if (Objects.nonNull(columnValue) && !columnValue.isEmpty()) {
                builder.columnsValues(columnValue);
                resultList.add(builder.build());
            }
        });
        return resultList;
    }

    public List<String> buildDeleteDml(RepairEntry repairEntry, Iterator<String> iterator) {
        final TableMetadata metadata = metaDataService.getMetaDataOfSchemaByCache(repairEntry.getTable());
        if (Objects.isNull(metadata)) {
            throw new BuildRepairStatementException(repairEntry.getTable());
        }
        final List<ColumnsMetaData> primaryMetas = metadata.getPrimaryMetas();
        List<String> resultList = new ArrayList<>();
        if (primaryMetas.size() == 1) {
            final ColumnsMetaData primaryMeta = primaryMetas.stream()
                                                            .findFirst()
                                                            .get();
            iterator.forEachRemaining(compositeKey -> {
                DeleteDmlBuilder deleteDmlBuilder =
                    new DeleteDmlBuilder(DataBaseType.OG, repairEntry.isOgCompatibility());
                final String deleteDml = deleteDmlBuilder.tableName(repairEntry.getTable())
                                                         .schema(repairEntry.getSchema())
                                                         .condition(primaryMeta, compositeKey)
                                                         .build();
                resultList.add(deleteDml);
            });
        } else {
            iterator.forEachRemaining(compositeKey -> {
                DeleteDmlBuilder deleteDmlBuilder =
                    new DeleteDmlBuilder(DataBaseType.OG, repairEntry.isOgCompatibility());
                resultList.add(deleteDmlBuilder.tableName(repairEntry.getTable())
                                               .schema(repairEntry.getSchema())
                                               .conditionCompositePrimary(compositeKey, primaryMetas)
                                               .build());
            });
        }
        return resultList;
    }

    protected Map<String, Map<String, String>> transtlateColumnValues(List<Map<String, String>> columnValues,
        List<ColumnsMetaData> primaryMetas) {
        final List<String> primaryKeys = getCompositeKeyColumns(primaryMetas);
        Map<String, Map<String, String>> map = new HashMap<>(InitialCapacity.CAPACITY_16);
        columnValues.forEach(values -> {
            map.put(getCompositeKey(values, primaryKeys), values);
        });
        return map;
    }

    private List<String> getCompositeKeyColumns(List<ColumnsMetaData> primaryMetas) {
        return primaryMetas.stream()
                           .map(ColumnsMetaData::getColumnName)
                           .collect(Collectors.toUnmodifiableList());
    }

    private String getCompositeKey(Map<String, String> columnValues, List<String> primaryKeys) {
        return primaryKeys.stream()
                          .map(key -> columnValues.get(key))
                          .collect(Collectors.joining(ExtConstants.PRIMARY_DELIMITER));
    }

    protected UpdateDmlBuilder createUpdateDmlBuilder(String targetSchema, boolean ogCompatibility,
        TableMetadata metadata) {
        UpdateDmlBuilder builder = new UpdateDmlBuilder(DataBaseType.OG, ogCompatibility);
        builder.metadata(metadata)
               .schema(targetSchema);
        return builder;
    }

}
