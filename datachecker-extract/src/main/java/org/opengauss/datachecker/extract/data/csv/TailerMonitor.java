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

package org.opengauss.datachecker.extract.data.csv;

import lombok.SneakyThrows;
import org.apache.commons.io.input.Tailer;
import org.opengauss.datachecker.common.entry.extract.SliceVo;

import java.util.concurrent.BlockingQueue;

/**
 * TailerMonitor
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/19
 * @since ：11
 */
public class TailerMonitor implements Runnable {
    private static final int MAX_QUEUE_SIZE = 10000;
    private final Tailer monitorTailer;
    private boolean monitor = true;
    private boolean isTailerWait = false;
    private final BlockingQueue<SliceVo> monitorListenerQueue;

    /**
     * create a monitor for the tailer thread ,to monitor the listenerQueue size.<br>
     * if listenerQueue.size is too large ,then wait the tailer thread; <br>
     * if listenerQueue.size is begging small ,then notify the tailer thread;
     *
     * @param tailer        tailer
     * @param listenerQueue listenerQueue
     */
    public TailerMonitor(Tailer tailer, BlockingQueue<SliceVo> listenerQueue) {
        this.monitorTailer = tailer;
        this.monitorListenerQueue = listenerQueue;
    }

    @SneakyThrows
    @Override
    public void run() {
        while (monitor) {
            if (!isTailerWait && monitorListenerQueue.size() >= MAX_QUEUE_SIZE) {
                synchronized (monitorTailer) {
                    monitorTailer.wait();
                    isTailerWait = true;
                }
            } else if (isTailerWait && monitorListenerQueue.size() <= MAX_QUEUE_SIZE / 2) {
                synchronized (monitorTailer) {
                    monitorTailer.notify();
                    isTailerWait = false;
                }
            }
            Thread.sleep(200);
        }
    }

    /**
     * stop tailer monitor
     */
    public void stop() {
        this.monitor = false;
    }
}
