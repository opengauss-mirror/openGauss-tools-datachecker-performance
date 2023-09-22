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

import org.apache.commons.collections4.CollectionUtils;
import org.opengauss.datachecker.common.entry.common.RepairEntry;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.config.DruidDataSourceConfig;
import org.opengauss.datachecker.extract.task.DataManipulationService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * JdbcRepairStatement
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/25
 * @since ：11
 */
@Component
@ConditionalOnBean(DruidDataSourceConfig.class)
public class JdbcRepairStatement extends BaseRepairStatement {
    @Resource
    private DataManipulationService dataManipulationService;

    @Override
    public List<String> buildRepairStatementInsertDml(RepairEntry repairEntry) {
        if (checkDiffEmpty(repairEntry)) {
            return new ArrayList<>();
        }
        repairStatementLog(repairEntry);
        final TableMetadata metadata = metaDataService.getMetaDataOfSchemaByCache(repairEntry.getTable());
        List<Map<String, String>> columnValues = dataManipulationService
            .queryColumnValues(repairEntry.getTable(), repairEntry.diffSetToList(), metadata);
        return buildInsertDml(repairEntry, metadata, columnValues, repairEntry.getDiffSet().iterator());
    }

    @Override
    public List<String> buildRepairStatementUpdateDml(RepairEntry repairEntry) {
        if (checkDiffEmpty(repairEntry)) {
            return new ArrayList<>();
        }
        repairStatementLog(repairEntry);
        final TableMetadata metadata = metaDataService.getMetaDataOfSchemaByCache(repairEntry.getTable());
        List<Map<String, String>> columnValues = dataManipulationService
            .queryColumnValues(repairEntry.getTable(), repairEntry.diffSetToList(), metadata);
        return buildUpdateDml(repairEntry, metadata, columnValues, repairEntry.getDiffSet().iterator());
    }

    @Override
    public List<String> buildRepairStatementDeleteDml(RepairEntry repairEntry) {
        if (checkDiffEmpty(repairEntry)) {
            return new ArrayList<>();
        }
        repairStatementLog(repairEntry);
        return buildDeleteDml(repairEntry, repairEntry.getDiffSet().iterator());
    }

    @Override
    protected void repairStatementLog(RepairEntry repairEntry) {
        int count = repairEntry.getDiffSet().size();
        log.info("check table[{}.{}] repair [{}] diff-count={} build repair dml", repairEntry.getSchema(),
            repairEntry.getTable(), repairEntry.getType(), count);
    }

    @Override
    protected boolean checkDiffEmpty(RepairEntry repairEntry) {
        return CollectionUtils.isEmpty(repairEntry.getDiffSet());
    }
}
