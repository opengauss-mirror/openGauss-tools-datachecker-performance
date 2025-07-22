/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITFOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.check.modules.check;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.TopicUtil;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

import cn.hutool.core.thread.ThreadUtil;

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
    private static final int MAX_RETRIES = 5;
    private static final long INITIAL_WAIT_MS = 100L;
    private static final long MAX_WAIT_MS = 5000L;

    private static String ROOT_PATH = "";

    /**
     * Export verification result
     *
     * @param result result
     */
    public static void export(CheckDiffResult result) {
        String fileName = getCheckResultFileName(result);
        FileUtils.deleteFile(fileName);
        try (FileChannel channel = FileChannel.open(Path.of(fileName), StandardOpenOption.CREATE,
            StandardOpenOption.WRITE); FileLock lock = channel.lock()) {
            FileUtils.writeFile(fileName, JsonObjectUtil.format(result));
            lock.release();
        } catch (IOException e) {
            LogUtils.error(log, "Export file [{}] failed: {}", fileName, e.getMessage());
        }
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

    /**
     * Backup verification result
     */
    public static void backCheckResultDirectory() {
        String checkResultPath = getResultPath();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Path.of(checkResultPath))) {
            final String backDir = getResultBakDir();
            FileUtils.createDirectories(backDir);
            for (Path file : stream) {
                Path target = Path.of(concat(backDir, file.getFileName().toString()));
                moveFileWithRetry(file, target);
            }
            LogUtils.info(log, "Backup verification result completed.");
        } catch (IOException e) {
            LogUtils.error(log, "Directory stream error: {}", e.getMessage());
        }
    }

    private static void moveFileWithRetry(Path source, Path target) {
        int retryCount = 0;
        long waitTime = INITIAL_WAIT_MS;
        boolean isMoved = false;
        while (!isMoved && retryCount < MAX_RETRIES) {
            isMoved = handleMoveByCopy(source, target);
            LogUtils.debug(log, "Moved file: retry->{}, {} -> {} {}", retryCount++, source, target, isMoved);
            ThreadUtil.safeSleep(waitTime);
            waitTime = Math.min(waitTime * 2, MAX_WAIT_MS);
        }
    }

    private static boolean handleMoveByCopy(Path source, Path target) {
        boolean isMoved = false;
        try {
            Files.copy(source, target);
            Files.deleteIfExists(source);
            LogUtils.warn(log, "Copied instead of moved: {} -> {}", source, target);
            isMoved = true;
        } catch (IOException e) {
            LogUtils.error(log, "Failed to copy file as fallback: {}", source, e);
        }
        return isMoved;
    }

    public static String getResultPath() {
        return ROOT_PATH.concat(CHECK_RESULT_PATH);
    }

    /**
     * get result backup root dir
     *
     * @return path
     */
    public static String getResultBakRootDir() {
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