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

import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.TaskUtil;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.cache.TableExtractStatusCache;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Data extraction task builder
 *
 * @author wang chao
 * @date 2022/5/8 19:27
 * @since 11
 **/
@Service
public class ExtractTaskBuilder {
    private static final String TASK_NAME_PREFIX = "extract_task_";

    /**
     * <pre>
     * Construct the table data extraction task according to the metadata cache information.
     * And initialize the execution state of the data extraction task.
     * Task construction depends on metadata cache information and the total number of current table records
     * loaded in the metadata cache.
     * The total number of query data of a single fragment task does not exceed {@value EXTRACT_MAX_ROW_COUNT}
     * {@code taskCountMap} It is used to count the number of tasks of fragment query of all tables to be extracted
     * {@code tableRows} Is the current table data amount counted in the table metadata information
     * </pre>
     *
     * @param tableNames Extraction task table set to be built
     * @return task list
     */
    public List<ExtractTask> builder(Set<String> tableNames) {
        List<ExtractTask> taskList = new ArrayList<>();
        final List<String> tableNameOrderList =
            tableNames.stream().filter(MetaDataCache::containsKey).sorted((tableName1, tableName2) -> {
                TableMetadata metadata1 = MetaDataCache.get(tableName1);
                TableMetadata metadata2 = MetaDataCache.get(tableName2);
                return (int) (metadata1.getTableRows() - metadata2.getTableRows());
            }).collect(Collectors.toList());

        // taskCountMap is used to count the number of tasks in table fragment query
        Map<String, Integer> taskCountMap = new HashMap<>(InitialCapacity.CAPACITY_1);
        tableNameOrderList.forEach(tableName -> {
            TableMetadata metadata = MetaDataCache.get(tableName);
            if (Objects.nonNull(metadata)) {
                taskList.add(buildTask(metadata));
                taskCountMap.put(tableName, 1);
            }
        });

        TableExtractStatusCache.init(taskCountMap);
        return taskList;
    }

    private ExtractTask buildTask(TableMetadata metadata) {
        return new ExtractTask().setTableMetadata(metadata).setOffset(metadata.getTableRows())
                                .setTableName(metadata.getTableName())
                                .setDivisionsTotalNumber(TaskUtil.calcAutoTaskCount(metadata.getTableRows()))
                                .setTaskName(taskNameBuilder(metadata.getTableName()));
    }

    /**
     * Task name build
     *
     * @param tableName tableName
     * @return task name
     */
    private String taskNameBuilder(@NonNull String tableName) {
        return TASK_NAME_PREFIX.concat(tableName);
    }
}
