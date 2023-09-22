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
