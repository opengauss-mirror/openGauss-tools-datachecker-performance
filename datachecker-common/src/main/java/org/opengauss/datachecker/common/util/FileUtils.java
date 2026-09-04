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

package org.opengauss.datachecker.common.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.exception.CheckingException;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * FileUtils
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class FileUtils {
    private static final int SMALL_FILE_THRESHOLD = 8192; // 8KB
    private static final Logger log = LogUtils.getLogger();

    /**
     * Fixed stripe write lock: the same path always maps to the same slot to ensure append mutual exclusion,
     * while writes to different files are mostly parallel;
     * lock memory is O(1), decoupled from the number of files written to disk (scale of business tables).
     * Rule of thumb for tuning: slot count >= 4 × the maximum number of concurrent writer threads in the whole process
     * (expected collision pairs = C(n,2)/slot count).
     * Currently there are about 25 concurrent writer threads
     * (check verification pool 10 + async event pool 10 + extract send pool 3 + a few scheduled tasks);
     * scale this value up accordingly when enlarging the worker thread pools
     * (e.g., 50 threads -> 256, 100 threads -> 512)
     */
    private static final int LOCK_STRIPE_COUNT = 128;
    private static final Object[] LOCK_STRIPES = new Object[LOCK_STRIPE_COUNT];

    static {
        for (int i = 0; i < LOCK_STRIPE_COUNT; i++) {
            LOCK_STRIPES[i] = new Object();
        }
    }

    private static Object lockFor(String filename) {
        String normalized = Paths.get(filename).toAbsolutePath().normalize().toString();
        return LOCK_STRIPES[(normalized.hashCode() & 0x7fffffff) % LOCK_STRIPE_COUNT];
    }

    /**
     * Creates a directory by creating all nonexistent parent directories first.
     *
     * @param path path
     */
    public static void createDirectories(String path) {
        File file = new File(path);
        if (!file.exists()) {
            try {
                Files.createDirectories(Paths.get(path));
            } catch (IOException e) {
                log.error("createDirectories error:", e);
            }
        }
    }

    /**
     * Write lines of text to a file. Characters are encoded into bytes using the UTF-8 charset.
     * This method works as if invoking it were equivalent to evaluating the expression:
     *
     * @param filename filename
     * @param content  content
     */
    public static void writeAppendFile(String filename, List<String> content) {
        synchronized (lockFor(filename)) {
            try {
                Files.write(Paths.get(filename), content, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            } catch (IOException e) {
                log.error("file write error:", e);
            }
        }
    }

    /**
     * Write lines of text to a file. Characters are encoded into bytes using the UTF-8 charset.
     *
     * @param filename filename
     * @param content  content
     */
    public static void writeAppendFile(String filename, Set<String> content) {
        synchronized (lockFor(filename)) {
            try {
                Files.write(Paths.get(filename), content, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            } catch (IOException e) {
                log.error("file write error:", e);
            }
        }
    }

    /**
     * Write lines of text to a file. Characters are encoded into bytes using the UTF-8 charset.
     *
     * @param filename filename
     * @param content  content
     */
    public static void writeAppendFile(String filename, String content) {
        synchronized (lockFor(filename)) {
            try {
                Path path = Paths.get(filename);
                if (path.getParent() != null) {
                    Files.createDirectories(path.getParent());
                }
                Files.write(path, content.getBytes(StandardCharsets.UTF_8),
                        StandardOpenOption.APPEND, StandardOpenOption.CREATE);
            } catch (IOException e) {
                log.error("file write error:", e);
            }
        }
    }

    /**
     * Write lines of text to a file. Characters are encoded into bytes using the UTF-8 charset.
     *
     * @param filename filename
     * @param content  content
     */
    public static void writeFile(String filename, String content) {
        synchronized (lockFor(filename)) {
            try {
                Path path = Paths.get(filename);
                if (path.getParent() != null) {
                    Files.createDirectories(path.getParent());
                }
                Files.write(path, content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE);
            } catch (IOException e) {
                log.error("file write error:", e);
            }
        }
    }

    /**
     * Load files under the specified path
     *
     * @param fileDirectory fileDirectory
     * @return file paths
     * @throws CheckingException CheckingException
     */
    public static List<Path> loadDirectory(String fileDirectory) throws CheckingException {
        try (Stream<Path> stream = Files.list(Paths.get(fileDirectory))) {
            return stream.collect(Collectors.toList());
        } catch (IOException e) {
            log.error("fileDirectory is not directory:", e);
            throw new CheckingException("can not load directory:" + fileDirectory);
        }
    }

    /**
     * Deletes a file if it exists.
     *
     * @param filename filename
     */
    public static void deleteFile(String filename) {
        try {
            Files.deleteIfExists(Paths.get(filename));
        } catch (IOException e) {
            log.error("file write error:", e);
        }
    }

    /**
     * Read the contents of the specified file
     *
     * @param filePath filePath
     * @return file content
     */
    public static String readFileContents(Path filePath) {
        try {
            if (Files.size(filePath) <= SMALL_FILE_THRESHOLD) {
                return Files.readString(filePath);
            }
            return readLargeFile(filePath);
        } catch (IOException e) {
            handleReadError(filePath, e);
            return StringUtils.EMPTY;
        }
    }

    private static String readLargeFile(Path filePath) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
            StringBuilder content = new StringBuilder();
            char[] buffer = new char[SMALL_FILE_THRESHOLD];
            int bytesRead;
            while ((bytesRead = reader.read(buffer)) != -1) {
                content.append(buffer, 0, bytesRead);
            }
            return content.toString();
        }
    }

    private static void handleReadError(Path filePath, IOException e) {
        String errorType;
        if (e instanceof NoSuchFileException) {
            errorType = "File Not Found";
        } else if (e instanceof AccessDeniedException) {
            errorType = "Permission Denied";
        } else {
            errorType = "Read Error";
        }
        log.error("[{}] Failed to read {}: {}", errorType, filePath, e.getMessage());
    }

    /**
     * rename csv data file to filename.csv.check
     *
     * @param csvDataPath csv data path
     * @param oldFileName csv file name
     * @return rename result boolean
     */
    public static boolean renameTo(String csvDataPath, String oldFileName) {
        File file = new File(csvDataPath, oldFileName);
        return file.exists() && file.renameTo(new File(csvDataPath, oldFileName + ".check"));
    }
}
