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

package org.opengauss.datachecker.check.annotation.aspect;

import org.apache.logging.log4j.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.aspectj.lang.reflect.MethodSignature;
import org.opengauss.datachecker.check.annotation.Statistical;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/**
 * StatisticalAspect
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/20
 * @since ：11
 */
@Component
@Aspect
public class StatisticalAspect {
    private static final Logger log = LogUtils.getLogger();
    @Value("${data.check.statistical-enable}")
    private boolean shouldStatistical;

    @Value("${data.check.data-path}")
    private String path;

    /**
     * statistical
     */
    @Pointcut("@annotation(org.opengauss.datachecker.check.annotation.Statistical)")
    public void statistical() {
        log.info("statistical annotation");
    }

    /**
     * doAround ProceedingJoinPoint
     *
     * @param pjp round aspect
     * @return method result
     * @throws Throwable exception
     */
    @Around("statistical()")
    public Object doAround(ProceedingJoinPoint pjp) throws Throwable {
        LocalDateTime start = LocalDateTime.now();
        Object ret = pjp.proceed();
        if (shouldStatistical) {
            logStatistical(pjp, start);
        }
        return ret;
    }

    private void logStatistical(ProceedingJoinPoint pjp, LocalDateTime start) {
        final Signature signature = pjp.getSignature();
        MethodSignature methodSignature = null;
        if (signature instanceof MethodSignature) {
            methodSignature = (MethodSignature) signature;
        }
        Method method = methodSignature.getMethod();
        Statistical statistical = method.getAnnotation(Statistical.class);
        if (!Objects.isNull(statistical)) {
            StatisticalRecord record = buildStatistical(statistical, start);
            FileUtils.writeAppendFile(getStatisticalFileName(), JsonObjectUtil.format(record));
        }
    }

    private String getStatisticalFileName() {
        return path.concat("statistical.txt");
    }

    private StatisticalRecord buildStatistical(Statistical statistical, LocalDateTime start) {
        LocalDateTime end = LocalDateTime.now();
        return new StatisticalRecord().setStart(JsonObjectUtil.formatTime(start)).setEnd(JsonObjectUtil.formatTime(end))
                                      .setCost(end.until(start, ChronoUnit.SECONDS)).setName(statistical.name());
    }
}
