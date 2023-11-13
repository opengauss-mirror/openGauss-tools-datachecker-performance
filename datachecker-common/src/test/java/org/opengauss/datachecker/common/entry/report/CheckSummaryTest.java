package org.opengauss.datachecker.common.entry.report;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.check.CheckTableInfo;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.util.JsonObjectUtil;

import java.time.LocalDateTime;
import java.util.List;

class CheckSummaryTest {

    private CheckSummary test;

    @BeforeEach
    void setUp() throws InterruptedException {
        test = new CheckSummary();

        test.setMode(CheckMode.FULL);

        test.setSuccessCount(90);
        test.setFailedCount(8);

        CheckTableInfo miss = new CheckTableInfo();
        miss.setSourceTableTotalSize(23);
        miss.setSinkTableTotalSize(22);
        miss.setSinkLoss(2);
        miss.setSinkExcess(1);
        miss.setMiss(3);
        miss.setLossTables(List.of("A", "B"));
        miss.setExcessTables(List.of("C"));
        test.setMissTable(miss);
        test.setTableCount(miss.fetchCheckedTableCount());
        test.setRowCount(100000);
        test.setCost(20);
        test.setStartTime(LocalDateTime.now().minusSeconds(test.getCost()));
        test.setEndTime(LocalDateTime.now());
    }

    @Test
    void testPrettyFormat() {
        System.out.println(JsonObjectUtil.prettyFormatMillis(test));
    }
}
