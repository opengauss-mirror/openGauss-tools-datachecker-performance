package org.opengauss.datachecker.extract.task;

import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

public class ExtractForkJoinTest {
    private static final Logger log = LogUtils.getLogger();
    @BeforeEach
    void setUp() {
    }

    @Test
    public void testParallelism1() throws ExecutionException, InterruptedException {
        int cupNum = Runtime.getRuntime().availableProcessors();
        log.info("CPU num:{}", cupNum);
        long firstNum = 1;
        long lastNum = 10000;
        List<Long> aList = LongStream.rangeClosed(firstNum, lastNum).boxed().collect(Collectors.toList());
        aList.parallelStream().forEach(e -> {
            log.info("输出:{}", e);
        });
    }

    @Test
    public void testQueryExtractForkJoinPool() {
        try {
            List<String> querySqlList = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                querySqlList.add("test_sql_select_" + i);
            }
            int dop = Math.min(querySqlList.size(), 5);
            ForkJoinPool customThreadPool = new ForkJoinPool(dop);
            customThreadPool.submit(() -> {
                querySqlList.parallelStream().map(sql -> {
                    log.error("{}", sql);
                    return sql + "  exe end";
                }).collect(Collectors.toList());
            }).get();
            System.out.println("执行结束");
        } catch (Exception ex) {
        }
    }

    @Test
    public void testQueryExtractForkJoinPool_CountDownLatch() {
        try {
            List<String> querySqlList = new ArrayList<>();
            for (int i = 0; i < 50; i++) {
                querySqlList.add("test_sql_select_" + i);
            }
            CountDownLatch countDownLatch = new CountDownLatch(querySqlList.size());
            int dop = Math.min(querySqlList.size(), 5);
            ForkJoinPool customThreadPool = new ForkJoinPool(dop);
            customThreadPool.submit(() -> {
                querySqlList.parallelStream().map(sql -> {
                    try {
                        log.error("{}", sql);
                        ThreadUtil.sleepHalfSecond();
                    } finally {
                        countDownLatch.countDown();
                        if (countDownLatch.getCount() > 0) {
                            log.error("exec sql [{}] remaining [{}] tasks", sql, countDownLatch.getCount());
                        }
                    }
                    return sql + "  exe end";
                }).collect(Collectors.toList());
            }).get();
            countDownLatch.await();
            System.out.println("执行结束");
        } catch (Exception ex) {
        }
    }

    @Test
    public void testParallelism4() {
        int cupNum = Runtime.getRuntime().availableProcessors();
        log.info("CPU num:{}", cupNum);
        long firstNum = 1;
        long lastNum = 10000;
        List<Long> aList = LongStream.rangeClosed(firstNum, lastNum).boxed().collect(Collectors.toList());
        ForkJoinPool forkJoinPool = new ForkJoinPool(8);
        try {
            List<Long> longs =
                forkJoinPool.submit(() -> aList.parallelStream().map(e -> e + 1).collect(Collectors.toList())).get();
            //通过调用get方法，等待任务执行完毕
            System.out.println(longs.size());
            System.out.println("执行结束");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            forkJoinPool.shutdown();
        }
    }
}
