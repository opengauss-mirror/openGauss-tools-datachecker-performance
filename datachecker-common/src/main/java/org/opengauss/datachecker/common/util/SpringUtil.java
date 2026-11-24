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

package org.opengauss.datachecker.common.util;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * SpringUtil
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/1
 * @since ：11
 */
@Component
public class SpringUtil implements ApplicationContextAware {
    private static ApplicationContext applicationContext;

    /**
     * set ApplicationContext
     *
     * @param applicationContext applicationContext
     * @throws BeansException BeansException
     */
    @Autowired
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        SpringUtil.applicationContext = applicationContext;
    }

    /**
     * get ApplicationContext
     *
     * @return applicationContext
     */
    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    /**
     * Get the corresponding bean instance according to the bean name
     *
     * @param name bean name
     * @return bean
     */
    public static Object getBean(String name) {
        return getApplicationContext().getBean(name);
    }

    /**
     * Get the corresponding bean instance according to the clazz type
     *
     * @param clazz clazz
     * @param <T>   clazz type
     * @return bean
     */
    public static <T> T getBean(Class<T> clazz) {
        return getApplicationContext().getBean(clazz);
    }

    /**
     * Get the corresponding bean instance according to the clazz type
     *
     * @param name  bean name
     * @param clazz clazz
     * @param <T>   clazz type
     * @return bean
     */
    public static <T> T getBean(String name, Class<T> clazz) {
        return getApplicationContext().getBean(name, clazz);
    }

    public static <T> Map<String, T> getBeans(Class<T> clazz) {
        return getApplicationContext().getBeansOfType(clazz);
    }
}