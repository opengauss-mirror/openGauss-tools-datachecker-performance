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

package org.opengauss.datachecker.common.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * FileUtils
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class FileUtils {
    private static final Logger log = LogUtils.getLogger();

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
    public static synchronized void writeAppendFile(String filename, List<String> content) {
        try {
            Files.write(Paths.get(filename), content, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        } catch (IOException e) {
            log.error("file write error:", e);
        }
    }

    /**
     * Write lines of text to a file. Characters are encoded into bytes using the UTF-8 charset.
     *
     * @param filename filename
     * @param content  content
     */
    public static synchronized void writeAppendFile(String filename, Set<String> content) {
        try {
            Files.write(Paths.get(filename), content, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        } catch (IOException e) {
            log.error("file write error:", e);
        }
    }

    /**
     * Write lines of text to a file. Characters are encoded into bytes using the UTF-8 charset.
     *
     * @param filename filename
     * @param content  content
     */
    public static synchronized void writeAppendFile(String filename, String content) {
        try {
            Files.write(Paths.get(filename), content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND,
                StandardOpenOption.CREATE);
        } catch (IOException e) {
            log.error("file write error:", e);
        }
    }

    /**
     * Write lines of text to a file. Characters are encoded into bytes using the UTF-8 charset.
     *
     * @param filename filename
     * @param content  content
     */
    public static synchronized void writeFile(String filename, String content) {
        try {
            Files.write(Paths.get(filename), content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE);
        } catch (IOException e) {
            log.error("file write error:", e);
        }
    }

    /**
     * Load files under the specified path
     *
     * @param fileDirectory fileDirectory
     * @return file paths
     */
    public static List<Path> loadDirectory(String fileDirectory) {
        try {
            final Path pathDirectory = Path.of(fileDirectory);
            if (Files.isDirectory(pathDirectory)) {
                return Files.list(pathDirectory)
                            .collect(Collectors.toList());
            } else {
                throw new NotDirectoryException(fileDirectory);
            }
        } catch (IOException e) {
            log.error("file write error:", e);
        }
        return new ArrayList<>(0);
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
            final byte[] bytes = Files.readAllBytes(filePath);
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.error("file read error:", e);
        }
        return StringUtils.EMPTY;
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
