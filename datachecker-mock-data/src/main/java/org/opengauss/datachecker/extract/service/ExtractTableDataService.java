/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-tools-datachecker-performance is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.extract.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ExtractTableDataService
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/26
 * @since ：11
 */
@Slf4j
@Service
public class ExtractTableDataService {
    @Autowired
    private JdbcTemplate jdbcTemplateMysql;
    @Autowired
    private JdbcTemplate jdbcTemplateOpenGauss;
    private QueryDataWapper queryDataWapper = new QueryDataWapper();

    /**
     * checkTable
     *
     * @param tableName tableName
     * @return result
     */
    public int checkTable(String tableName) {
        AtomicInteger cnt = new AtomicInteger(0);
        final List<String> mysqlList =
            queryDataWapper.queryPrimaryValues(jdbcTemplateMysql, TableSqlWapper.SELECT_PRI_M, tableName);
        log.info("query  mysql : table={}, row-size={} ", tableName, mysqlList.size());
        final List<String> openGaussList =
            queryDataWapper.queryPrimaryValues(jdbcTemplateOpenGauss, TableSqlWapper.SELECT_PRI_O, tableName);
        log.info("query openGauss : table={}, row-size={} ", tableName, openGaussList.size());
        HashHandler source = new HashHandler();
        HashHandler sink = new HashHandler();
        mysqlList.parallelStream().forEach(parmary -> {
            final long sourceHash = source.xx3Hash(parmary);
            final long sinkHash = sink.xx3Hash(parmary);
            if (sourceHash != sinkHash) {
                log.info("hash difference : key={},hash source={} : sink={}", parmary, sourceHash, sinkHash);
                cnt.incrementAndGet();
            }
        });
        log.info("{} key ,hash calc finished ", tableName);
        mysqlList.parallelStream().filter(mysqlKey -> !openGaussList.contains(mysqlKey)).forEach(reduce -> {
            log.info("mysql row not found in openGauss -> {}", reduce);
        });
        log.info("{} mysql row found finished ", tableName);
        openGaussList.parallelStream().filter(openGauss -> !mysqlList.contains(openGauss)).forEach(reduce -> {
            log.info("openGauss row not fount in mysql -> {}", reduce);
        });
        log.info("{} openGauss row found finished ", tableName);

        List<Long> mysqlHashList = new ArrayList<>();
        mysqlList.forEach(mysqlKey -> {
            mysqlHashList.add(source.xx3Hash(mysqlKey));
        });
        final HashSet<Long> mysqlHashSet = new HashSet<>(mysqlHashList);
        log.info("mysql row  hash list -> {} , set->{}", mysqlHashList.size(), mysqlHashSet.size());

        List<Long> openGaussHashList = new ArrayList<>();
        openGaussList.forEach(openGauss -> {
            openGaussHashList.add(sink.xx3Hash(openGauss));
        });
        final HashSet<Long> openGaussHashSet = new HashSet<>(openGaussHashList);
        log.info("openGauss row  hash list -> {} , set->{}", openGaussHashList.size(), openGaussHashSet.size());
        Object lock = new Object();
        List<Long> openGaussParallelHashList = new ArrayList<>();
        openGaussList.parallelStream().forEach(openGauss -> {
            final long hash = sink.xx3Hash(openGauss);
            synchronized (lock) {
                openGaussParallelHashList.add(hash);
            }
        });
        final HashSet<Long> openGaussParallelHashSet = new HashSet<>(openGaussParallelHashList);
        log.info("openGauss row Parallel hash list -> {} , set->{}", openGaussParallelHashList.size(),
            openGaussParallelHashList.size());
        return cnt.get();
    }
}
