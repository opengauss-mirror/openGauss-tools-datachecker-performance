package org.opengauss.datachecker.common.service;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.SpringUtil;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.boot.SpringApplication;
import org.springframework.scheduling.concurrent.ExecutorConfigurationSupport;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author ：wangchao
 * @date ：Created in 2023/2/14
 * @since ：11
 */
@Service
public class ShutdownService {
    private static final Logger log = LogUtils.getLogger();
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final AtomicInteger monitor = new AtomicInteger(0);
    private List<ExecutorService> executorServiceList = new LinkedList<>();
    private List<ThreadPoolTaskExecutor> threadExecutorList = new LinkedList<>();
    @Resource
    private DynamicThreadPoolManager dynamicThreadPoolManager;

    public void shutdown(String message) {
        log.info("The check server will be shutdown , {} . check server exited .", message);
        isShutdown.set(true);
        ThreadUtil.killThreadByName("kafka-producer-network-thread");

        dynamicThreadPoolManager.closeDynamicThreadPoolMonitor();
        while (monitor.get() > 0) {
            ThreadUtil.sleepHalfSecond();
        }
        threadExecutorList.forEach(ExecutorConfigurationSupport::shutdown);
        executorServiceList.forEach(ExecutorService::shutdownNow);
        ThreadUtil.sleepHalfSecond();
        System.exit(SpringApplication.exit(SpringUtil.getApplicationContext()));
    }

    public boolean isShutdown() {
        return isShutdown.get();
    }

    public synchronized int addMonitor() {
        return monitor.incrementAndGet();
    }

    public synchronized int releaseMonitor() {
        return monitor.decrementAndGet();
    }

    public void addExecutorService(ExecutorService scheduledExecutor) {
        executorServiceList.add(scheduledExecutor);
    }

    public void addThreadPoolTaskExecutor(ThreadPoolTaskExecutor executor) {
        threadExecutorList.add(executor);
    }
}
