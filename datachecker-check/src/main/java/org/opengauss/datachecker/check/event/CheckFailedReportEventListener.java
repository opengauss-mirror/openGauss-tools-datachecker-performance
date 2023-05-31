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

package org.opengauss.datachecker.check.event;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.check.load.CheckEnvironment;
import org.opengauss.datachecker.check.modules.check.CheckDiffResult;
import org.opengauss.datachecker.check.modules.check.CheckResultConstants;
import org.opengauss.datachecker.common.entry.report.CheckFailed;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Objects;
import java.util.Set;

/**
 * @author ：wangchao
 * @date ：Created in 2023/3/7
 * @since ：11
 */
@Slf4j
@Component
public class CheckFailedReportEventListener extends CheckReportEventAdapter
    implements ApplicationListener<CheckFailedReportEvent> {

    private static final String FAILED_LOG_NAME = CheckResultConstants.FAILED_LOG_NAME;
    private static final int MAX_DISPLAY_SIZE = 200;
    @Resource
    private CheckEnvironment checkEnvironment;

    @Override
    public void onApplicationEvent(CheckFailedReportEvent event) {
        final CheckDiffResult source = (CheckDiffResult) event.getSource();
        FileUtils
            .writeAppendFile(getFailedPath(), JsonObjectUtil.prettyFormatMillis(translateCheckFailed(source)) + ",");
        log.debug("completed {} failed success and export results of  ", source.getTable());
    }

    private String getFailedPath() {
        return getLogRootPath(checkEnvironment.getExportCheckPath()) + FAILED_LOG_NAME;
    }

    private CheckFailed translateCheckFailed(CheckDiffResult result) {
        long cost = calcCheckTaskCost(result.getStartTime(), result.getEndTime());
        StringBuffer hasMore = new StringBuffer();
        return new CheckFailed().setProcess(result.getProcess()).setSchema(result.getSchema())
                                .setTopic(new String[] {result.getTopic()}).setPartition(result.getPartitions())
                                .setBeginOffset(result.getBeginOffset()).setTableName(result.getTable()).setCost(cost)
                                .setDiffCount(result.getTotalRepair()).setEndTime(result.getEndTime())
                                .setStartTime(result.getStartTime())
                                .setKeyInsertSet(getKeyList(result.getKeyInsertSet(), hasMore, "insert key has more;"))
                                .setKeyDeleteSet(getKeyList(result.getKeyDeleteSet(), hasMore, "delete key has more;"))
                                .setKeyUpdateSet(getKeyList(result.getKeyUpdateSet(), hasMore, "update key has more;"))
                                .setMessage(result.getMessage()).setHasMore(hasMore.toString());
    }

    private Set<String> getKeyList(Set<String> keySet, StringBuffer hasMore, String message) {
        if (Objects.isNull(keySet) || keySet.size() <= MAX_DISPLAY_SIZE) {
            return keySet;
        }
        hasMore.append(message);
        return Sets.newTreeSet(Iterables.limit(keySet, MAX_DISPLAY_SIZE));
    }
}
