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

package org.opengauss.datachecker.extract.util;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.util.LogUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * OutOfMemorySimulator
 *
 * @author: wangchao
 * @Date: 2025/7/15 10:28
 * @since 7.0.0-RC2
 **/
public class OutOfMemorySimulator {
    private static final Logger LOG = LogUtils.getLogger();
    private static final List<byte[]> GLOBAL_MEMORY_HOG = new ArrayList<>();
    private static final Object LOCK = new Object();

    /**
     * Simulate memory allocation
     *
     * @param name task name
     * @param sliceNo slice number
     */
    public static void allocateMemory(String name, int sliceNo) {
        int globalCount = 0;
        try {
            while (true) {
                byte[] chunk = new byte[1024 * 1024];
                synchronized (LOCK) {
                    GLOBAL_MEMORY_HOG.add(chunk);
                    globalCount = GLOBAL_MEMORY_HOG.size();
                }
                if (globalCount % 5 == 0) {
                    LOG.info("[task #{}#{}] memory allocate: {} block, total={}MB%n", name, sliceNo, globalCount,
                        globalCount * 2);
                }
                Thread.sleep(Math.max(10, 100 - globalCount));
            }
        } catch (OutOfMemoryError e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
