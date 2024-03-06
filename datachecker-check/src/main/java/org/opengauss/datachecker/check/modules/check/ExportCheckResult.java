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

package org.opengauss.datachecker.check.modules.check;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.TopicUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

/**
 * ExportCheckResult
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/17
 * @since ：11
 */
public class ExportCheckResult {
    private static final Logger log = LogUtils.getLogger(ExportCheckResult.class);
    private static final DateTimeFormatter FORMATTER_DIR = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");
    private static final String CHECK_RESULT_BAK_DIR = File.separator + "result_bak" + File.separator;
    private static final String CHECK_RESULT_PATH = File.separator + "result" + File.separator;

    private static String ROOT_PATH = "";

    public static void export(CheckDiffResult result) {
        String fileName = getCheckResultFileName(result);
        FileUtils.deleteFile(fileName);
        FileUtils.writeFile(fileName, JsonObjectUtil.format(result));
    }

    private static String getCheckResultFileName(CheckDiffResult result) {
        String fileName = getBaseFileName(result);
        if (Objects.equals(result.getCheckMode(), CheckMode.FULL)) {
            fileName = fileName + "_" + result.getPartitions();
        }
        fileName = fileName + ".txt";
        return getResultPath().concat(fileName);
    }

    private static String getBaseFileName(CheckDiffResult result) {
        return result.getProcess() + "_" + TopicUtil.getTableWithLetter(result.getTable());
    }

    /**
     * Initialize the verification result environment
     *
     * @param path Verification result output path
     */
    public static void initEnvironment(String path) {
        ROOT_PATH = path;
        String checkResultPath = getResultPath();
        FileUtils.createDirectories(checkResultPath);
        FileUtils.createDirectories(getResultBakRootDir());
    }

    public static void backCheckResultDirectory() {
        String checkResultPath = getResultPath();
        final List<Path> resultPaths = FileUtils.loadDirectory(checkResultPath);
        if (CollectionUtils.isEmpty(resultPaths)) {
            return;
        }
        final String backDir = getResultBakDir();
        FileUtils.createDirectories(backDir);
        resultPaths.forEach(file -> {
            try {
                Files.move(file, Path.of(concat(backDir, file.getFileName()
                                                             .toString())), ATOMIC_MOVE);
            } catch (IOException e) {
                LogUtils.error(log, "back the verification result environment error");
            }
        });
        LogUtils.info(log, "back the verification result.");
    }

    public static String getResultPath() {
        return ROOT_PATH.concat(CHECK_RESULT_PATH);
    }

    private static String getResultBakRootDir() {
        return ROOT_PATH.concat(CHECK_RESULT_BAK_DIR);
    }

    private static String concat(String dir, String fileName) {
        return dir.concat(File.separator)
                  .concat(fileName);
    }

    private static String getResultBakDir() {
        return getResultBakRootDir().concat(FORMATTER_DIR.format(LocalDateTime.now()));
    }
}
