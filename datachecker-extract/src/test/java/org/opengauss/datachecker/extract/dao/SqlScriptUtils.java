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

package org.opengauss.datachecker.extract.dao;

import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.opengauss.datachecker.common.exception.ExtractJuintTestException;
import org.springframework.core.io.ClassPathResource;

import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;

/**
 * test utils: SqlScriptUtils
 *
 * @author ：wangchao
 * @date ：Created in 2024/1/25
 * @since ：11
 */
@Slf4j
public class SqlScriptUtils {
    /**
     * execTestSqlScript
     *
     * @param scriptPath scriptPath
     */
    public static void execTestSqlScript(Connection connection, String scriptPath) {
        try {
            ScriptRunner sc = new ScriptRunner(connection);
            Reader reader = new InputStreamReader(new ClassPathResource(scriptPath).getInputStream());
            sc.runScript(reader);
        } catch (Exception exc) {
            log.error("load test sql script error:", exc);
            throw new ExtractJuintTestException("load test sql script error : " + scriptPath);
        }
    }
}
