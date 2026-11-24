/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-tools-datachecker-performance is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.extract.service;

import org.apache.commons.collections4.CollectionUtils;
import org.opengauss.datachecker.common.entry.check.Difference;
import org.opengauss.datachecker.common.entry.common.RepairEntry;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.config.CsvSourceConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * CsvRepairStatement
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/25
 * @since ：11
 */
@Component
@ConditionalOnBean({CsvSourceConfiguration.class})
public class CsvRepairStatement extends BaseRepairStatement {
    @Override
    public List<String> buildRepairStatementInsertDml(RepairEntry repairEntry) {
        repairStatementLog(repairEntry);
        if (checkDiffEmpty(repairEntry)) {
            return new ArrayList<>();
        }
        List<Map<String, String>> diffValues =
            dataAccessService.query(repairEntry.getTable(), repairEntry.getFileName(), repairEntry.getDiffList());
        final TableMetadata metadata = metaDataService.getMetaDataOfSchemaByCache(repairEntry.getTable());
        List<String> insertKeys = getDifferenceKeyList(repairEntry);
        return buildInsertDml(repairEntry, metadata, diffValues, insertKeys.iterator());
    }

    @Override
    public List<String> buildRepairStatementUpdateDml(RepairEntry repairEntry) {
        repairStatementLog(repairEntry);
        if (checkDiffEmpty(repairEntry)) {
            return new ArrayList<>();
        }
        List<Map<String, String>> diffValues =
            dataAccessService.query(repairEntry.getTable(), repairEntry.getFileName(), repairEntry.getDiffList());
        final TableMetadata metadata = metaDataService.getMetaDataOfSchemaByCache(repairEntry.getTable());
        List<String> updateKeys = getDifferenceKeyList(repairEntry);
        return buildUpdateDml(repairEntry, metadata, diffValues, updateKeys.iterator());
    }

    @Override
    public List<String> buildRepairStatementDeleteDml(RepairEntry repairEntry) {
        repairStatementLog(repairEntry);
        if (checkDiffEmpty(repairEntry)) {
            return new ArrayList<>();
        }
        List<String> deleteKeys = getDifferenceKeyList(repairEntry);
        return buildDeleteDml(repairEntry, deleteKeys.iterator());
    }

    private List<String> getDifferenceKeyList(RepairEntry repairEntry) {
        return repairEntry.getDiffList().stream().map(Difference::getKey).collect(Collectors.toList());
    }

    @Override
    protected void repairStatementLog(RepairEntry repairEntry) {
        int count = repairEntry.getDiffList().size();
        log.info("check table[{}.{}] repair [{}] diff-count={} build repair dml", repairEntry.getSchema(),
            repairEntry.getTable(), repairEntry.getType(), count);
    }

    @Override
    protected boolean checkDiffEmpty(RepairEntry repairEntry) {
        return CollectionUtils.isEmpty(repairEntry.getDiffList());
    }
}
