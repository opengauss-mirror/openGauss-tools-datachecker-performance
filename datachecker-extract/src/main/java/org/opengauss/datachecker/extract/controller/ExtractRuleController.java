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

package org.opengauss.datachecker.extract.controller;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.common.DistributeRuleEntry;
import org.opengauss.datachecker.extract.cache.TopicCache;
import org.opengauss.datachecker.extract.load.EnvironmentLoader;
import org.opengauss.datachecker.extract.service.RuleAdapterService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * Filter Rules service
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/23
 * @since ：11
 */
@Slf4j
@RestController
public class ExtractRuleController {
    @Resource
    private RuleAdapterService ruleAdapterService;
    @Resource
    private EnvironmentLoader environmentLoader;
    @Resource
    private TopicCache topicCache;

    /**
     * Distribution Data Extraction Filter Rules
     *
     * @param rules rules
     */
    @PostMapping("/extract/rules/distribute")
    public void distributeRules(@RequestBody DistributeRuleEntry rules) {
        ruleAdapterService.init(rules.getRules());
        environmentLoader.load(rules.getCheckMode());
    }

    @PostMapping("/notify/check/finished")
    public void notifyCheckTableFinished(@RequestParam(name = "tableName") String tableName) {
        topicCache.removeTopic(tableName);
        log.info("remove topic cache of table [{}]", tableName);
    }
}
