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

import org.opengauss.datachecker.common.entry.common.RepairEntry;

import java.util.List;

/**
 * @author ：wangchao
 * @date ：Created in 2023/8/25
 * @since ：11
 */
public interface RepairStatement {
    /**
     * Build repair insert statements
     *
     * @param repairEntry repairEntry
     * @return dml
     */
    List<String> buildRepairStatementInsertDml(RepairEntry repairEntry);

    /**
     * Build repair update statements
     *
     * @param repairEntry repairEntry
     * @return dml
     */
    List<String> buildRepairStatementUpdateDml(RepairEntry repairEntry);

    /**
     * Build repair delete statements
     *
     * @param repairEntry repairEntry
     * @return dml
     */
    List<String> buildRepairStatementDeleteDml(RepairEntry repairEntry);
}
