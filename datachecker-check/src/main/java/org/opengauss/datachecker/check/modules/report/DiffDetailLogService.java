/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
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

package org.opengauss.datachecker.check.modules.report;

import com.alibaba.fastjson.JSONException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.modules.check.CheckDiffResult;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.check.Difference;
import org.opengauss.datachecker.common.entry.common.RepairEntry;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.Database;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.common.util.TopicUtil;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import jakarta.annotation.Resource;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Diff detail log service, an auxiliary feature independent of the main check flow.
 * When the switch is on and the check completes, takes the first N diff rows of each diff
 * type in primary key order, queries back the actual column values from the source and
 * sink sides respectively, and writes them to a separate diff_detail log file for
 * troubleshooting.
 *
 * @author: xujintao
 * @date: Created in 2026/9/7
 * @since: 11
 */
@Service
public class DiffDetailLogService {
    private static final Logger log = LogUtils.getLogger();
    private static final int MAX_DIFF_DETAIL_PER_TABLE = 10;
    private static final String DIFF_DETAIL_LOG_TEMPLATE = "diff_detail_%s_%s.txt";

    /**
     * Wait time (ms) before the post-check diff query-back runs, used to stagger the heap
     * peak of the finishing phase
     */
    private static final long DELAY_MILLIS = 3000L;

    // Keep in sync with the extract side's ResultSetHandler.RAW_VALUE_SUFFIX; distinguishes
    // pre-mapping raw values
    private static final String RAW_VALUE_SUFFIX = "__raw";

    @Resource
    private FeignClientService feignClient;

    /**
     * Write the diff detail log for all failed check results.
     * Does nothing when the switch is off or the failed list is empty.
     *
     * @param failedList results of all failed table checks
     */
    public void writeDiffDetailLog(List<CheckDiffResult> failedList) {
        if (!ConfigCache.getBooleanValue(ConfigConstants.DIFF_DEBUG_LOG_ENABLED)
            || CollectionUtils.isEmpty(failedList)) {
            return;
        }
        try {
            String logFilePath = ConfigCache.getCheckResult();
            // Group by table, preserving insertion order
            Map<String, List<CheckDiffResult>> tableGrouped = failedList.stream()
                .collect(Collectors.groupingBy(CheckDiffResult::getTable, LinkedHashMap::new,
                    Collectors.toList()));
            tableGrouped.forEach((table, tableFailedList) ->
                writeTableDiffDetail(logFilePath, tableFailedList));
        } catch (JSONException | NullPointerException exception) {
            log.error("write diff detail log failed", exception);
        }
    }

    /**
     * Run the diff detail query-back log with a delay, staggering the three-process heap
     * peak at the end of the check.
     * <p>
     * The moment the check completes, all three processes are at their heap peak (the check
     * side holds the full check results, and the extract sides' Kafka buffers and temporary
     * objects are not yet released); starting the query-back then easily triggers OOM at the
     * high-water point, hence the delayed execution.
     * Returns immediately without delay when the switch is off or there are no failed results.
     * <p>
     * Note: this method blocks synchronously; the caller must wait for it to finish before
     * triggering process shutdown, to avoid the Feign query-back calls being interrupted by
     * the extract processes shutting down and losing half-written logs.
     *
     * @param failedList results of all failed table checks
     */
    public void writeDiffDetailLogDelay(List<CheckDiffResult> failedList) {
        if (!ConfigCache.getBooleanValue(ConfigConstants.DIFF_DEBUG_LOG_ENABLED)
            || CollectionUtils.isEmpty(failedList)) {
            return;
        }
        LogUtils.info(log, "diff detail log delayed {}ms to avoid heap peak, failed result count={}",
            DELAY_MILLIS, failedList.size());
        ThreadUtil.sleep(DELAY_MILLIS);
        writeDiffDetailLog(failedList);
    }

    private void writeTableDiffDetail(String logFilePath, List<CheckDiffResult> tableFailedList) {
        CheckDiffResult tableFailed = tableFailedList.get(0);
        String detailFile = logFilePath + getDiffDetailFileName(tableFailed);
        List<String> lines = new ArrayList<>();
        lines.add("================ diff detail: " + tableFailed.getSchema() + "." + tableFailed.getTable()
            + " (top " + MAX_DIFF_DETAIL_PER_TABLE + " per type) ================");
        appendDiffDetailSection(lines, tableFailed, "UPDATE",
            collectKeys(tableFailedList, CheckDiffResult::getKeyUpdateSet, CheckDiffResult::getKeyUpdate));
        appendDiffDetailSection(lines, tableFailed, "INSERT (source has, sink missing)",
            collectKeys(tableFailedList, CheckDiffResult::getKeyInsertSet, CheckDiffResult::getKeyInsert));
        appendDiffDetailSection(lines, tableFailed, "DELETE (sink has, source missing)",
            collectKeys(tableFailedList, CheckDiffResult::getKeyDeleteSet, CheckDiffResult::getKeyDelete));
        FileUtils.writeAppendFile(detailFile, lines);
    }

    private void appendDiffDetailSection(List<String> lines, CheckDiffResult tableFailed, String type,
        Set<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return;
        }
        List<String> topKeys = new ArrayList<>(keys);
        RepairEntry entry = buildQueryEntry(tableFailed, topKeys);
        Map<String, Map<String, String>> sourceValues = feignClient.queryColumnValues(Endpoint.SOURCE, entry);
        Map<String, Map<String, String>> sinkValues = feignClient.queryColumnValues(Endpoint.SINK, entry);
        lines.add("--- " + type + " diff (top " + topKeys.size() + ") ---");
        for (String key : topKeys) {
            Map<String, String> sourceRow = sourceValues.get(key);
            Map<String, String> sinkRow = sinkValues.get(key);
            lines.add("[key=" + key + "]");
            lines.add("  SOURCE: " + formatColumnValues(sourceRow));
            lines.add("  SINK  : " + formatColumnValues(sinkRow));
            // Compare column by column, highlight mismatched columns and tag the diff type
            // (null / empty string / different values) for quick root-cause locating
            lines.add("  DIFF  : " + formatColumnDiff(sourceRow, sinkRow));
        }
        lines.add("");
    }

    private String formatColumnValues(Map<String, String> values) {
        if (values == null || values.isEmpty()) {
            return "(not found)";
        }
        // Show mapped values (DB) and pre-mapping raw values separately, to see whether the
        // mapping performed precision truncation
        Map<String, String> mapped = new LinkedHashMap<>();
        Map<String, String> raw = new LinkedHashMap<>();
        values.forEach((key, value) -> {
            if (key.endsWith(RAW_VALUE_SUFFIX)) {
                raw.put(key.substring(0, key.length() - RAW_VALUE_SUFFIX.length()), value);
            } else {
                mapped.put(key, value);
            }
        });
        String mappedStr = JsonObjectUtil.formatSimple(mapped);
        if (raw.isEmpty()) {
            return mappedStr;
        }
        return "mapped=" + mappedStr + " | raw(DB)=" + JsonObjectUtil.formatSimple(raw);
    }

    private String formatColumnDiff(Map<String, String> sourceRow, Map<String, String> sinkRow) {
        if (sourceRow == null || sinkRow == null || sourceRow.isEmpty() || sinkRow.isEmpty()) {
            return "(cannot compare column by column: source or sink data missing)";
        }
        Set<String> allColumns = new TreeSet<>();
        allColumns.addAll(sourceRow.keySet());
        allColumns.addAll(sinkRow.keySet());
        List<String> diffs = new ArrayList<>();
        for (String col : allColumns) {
            // Skip pre-mapping raw value columns; compare only the mapped actual column values
            if (col.endsWith(RAW_VALUE_SUFFIX)) {
                continue;
            }
            String srcVal = sourceRow.get(col);
            String snkVal = sinkRow.get(col);
            if (Objects.equals(srcVal, snkVal)) {
                continue;
            }
            String diffType;
            if (srcVal == null && snkVal != null) {
                diffType = snkVal.isEmpty() ? "NULL_vs_EMPTY" : "NULL_vs_VALUE";
            } else if (srcVal != null && snkVal == null) {
                diffType = srcVal.isEmpty() ? "EMPTY_vs_NULL" : "VALUE_vs_NULL";
            } else if (srcVal.isEmpty() && !snkVal.isEmpty()) {
                diffType = "EMPTY_vs_VALUE";
            } else if (!srcVal.isEmpty() && snkVal.isEmpty()) {
                diffType = "VALUE_vs_EMPTY";
            } else {
                diffType = "VALUE_DIFF";
            }
            diffs.add(col + "[" + diffType + ": src=" + truncateVal(srcVal) + ", snk=" + truncateVal(snkVal) + "]");
        }
        if (diffs.isEmpty()) {
            return "(all column values are equal - possibly a hash collision or data changed between sampling times)";
        }
        return String.join(" | ", diffs);
    }

    private String truncateVal(String val) {
        if (val == null) {
            return "null";
        }
        if (val.isEmpty()) {
            return "(empty string)";
        }
        return val.length() <= 60 ? val : val.substring(0, 60) + "...(" + val.length() + ")";
    }

    /**
     * Collect diff primary keys from the failed results of all slices of the same table
     * (deduplicated, natural key order); stop iterating once the cap is reached, so a table
     * with huge diffs does not copy every key into a TreeSet and amplify memory at the
     * check's closing heap high-water point.
     *
     * @param tableFailedList failed check results of the same table
     * @param keySetExtractor extractor for the diff primary key set field
     * @param keyListExtractor extractor for the diff detail list field
     * @return the deduplicated, sorted diff primary key set, capped in size
     */
    private Set<String> collectKeys(List<CheckDiffResult> tableFailedList,
        java.util.function.Function<CheckDiffResult, Set<String>> keySetExtractor,
        java.util.function.Function<CheckDiffResult, List<Difference>> keyListExtractor) {
        Set<String> keys = new TreeSet<>();
        for (CheckDiffResult failed : tableFailedList) {
            if (keys.size() >= MAX_DIFF_DETAIL_PER_TABLE) {
                break;
            }
            addKeySetCapped(keys, keySetExtractor.apply(failed));
            addDifferenceKeysCapped(keys, keyListExtractor.apply(failed));
        }
        return keys;
    }

    private void addKeySetCapped(Set<String> keys, Set<String> keySet) {
        if (CollectionUtils.isEmpty(keySet)) {
            return;
        }
        for (String key : keySet) {
            if (keys.size() >= MAX_DIFF_DETAIL_PER_TABLE) {
                break;
            }
            keys.add(key);
        }
    }

    private void addDifferenceKeysCapped(Set<String> keys, List<Difference> keyList) {
        if (CollectionUtils.isEmpty(keyList)) {
            return;
        }
        for (Difference diff : keyList) {
            if (keys.size() >= MAX_DIFF_DETAIL_PER_TABLE) {
                break;
            }
            keys.add(diff.getKey());
        }
    }

    private RepairEntry buildQueryEntry(CheckDiffResult tableFailed, List<String> keys) {
        RepairEntry entry = new RepairEntry();
        BeanUtils.copyProperties(tableFailed, entry);
        Database sinkDatabase = ConfigCache.getValue(ConfigConstants.DATA_CHECK_SINK_DATABASE, Database.class);
        if (Objects.nonNull(sinkDatabase)) {
            entry.setSchema(sinkDatabase.getSchema());
        }
        entry.setDiffSet(new TreeSet<>(keys));
        return entry;
    }

    private String getDiffDetailFileName(CheckDiffResult tableFailed) {
        final String schema = tableFailed.getSchema();
        final String table = TopicUtil.getTableWithLetter(tableFailed.getTable());
        return String.format(DIFF_DETAIL_LOG_TEMPLATE, schema, table);
    }
}
