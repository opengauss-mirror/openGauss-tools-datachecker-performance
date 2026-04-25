/*
 * Copyright (c) 2024-2024 Huawei Technologies Co.,Ltd.
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

package org.opengauss.datachecker.check.service;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import jakarta.annotation.PreDestroy;
import jakarta.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.FileVisitResult;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * IncrementLogManager
 *
 * @author wang chao
 * @since 2022/5/8 19:17
 **/
@Service
public class IncrementLogManager {
    private static final Logger log = LogUtils.getLogger(IncrementLogManager.class);
    private static final int MAX_BACK_DIR_NUM = 10;
    private static final long MONITOR_INTERVAL_MS = 5000L;

    @Resource
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;
    private boolean isWatching = true;

    /**
     * init log dir and register watch service
     *
     * @param path path
     */
    public void init(String path) {
        bakResultLogMonitor(path);
    }

    /**
     * monitor the result log file
     *
     * @param path path
     */
    private void bakResultLogMonitor(String path) {
        CheckMode checkMode = ConfigCache.getCheckMode();
        if (!Objects.equals(checkMode, CheckMode.INCREMENT)) {
            log.info("debezium-enable is false, not monitor result log file");
            return;
        }
        threadPoolTaskExecutor.submit(() -> {
            while (isWatching) {
                try {
                    monitorDirectory(path);
                } catch (CheckingException e) {
                    LogUtils.error(log, "Error monitoring directory: {}", e.getMessage());
                }
                try {
                    Thread.sleep(MONITOR_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
    }

    /**
     * stop watch
     */
    @PreDestroy
    public void destroy() {
        isWatching = false;
    }

    private void monitorDirectory(String path) {
        Path dir = Paths.get(path);
        File[] files = dir.toFile().listFiles();
        if (files == null || files.length == 0) {
            return;
        }
        LinkedList<Path> sortedDirs = Arrays.stream(files)
                .map(File::toPath)
                .sorted()
                .collect(Collectors.toCollection(LinkedList::new));
        while (sortedDirs.size() > MAX_BACK_DIR_NUM) {
            Path delDir = sortedDirs.removeFirst();
            try {
                deleteDir(delDir);
                LogUtils.warn(log, "remove result back more dir : {}", delDir);
            } catch (IOException e) {
                LogUtils.error(log, "remove result back more dir : ", e.getMessage());
                sortedDirs.addLast(delDir);
            }
        }
    }

    private static void deleteDir(Path dir) throws IOException {
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc == null) {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                } else {
                    throw exc;
                }
            }
        });
    }
}
