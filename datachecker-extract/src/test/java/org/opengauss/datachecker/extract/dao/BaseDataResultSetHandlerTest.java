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

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.hamcrest.CoreMatchers;
import org.opengauss.datachecker.common.entry.extract.PrimaryColumnBean;
import org.opengauss.datachecker.common.exception.ExpectTableDataNotFountException;
import org.opengauss.datachecker.common.exception.ExtractJuintTestException;
import org.opengauss.datachecker.extract.data.mapper.MetaDataMapper;
import org.opengauss.datachecker.extract.task.ResultSetHandler;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsMapContaining.hasEntry;

/**
 * BaseDataResultSetHandlerTest
 *
 * @author ：wangchao
 * @date ：Created in 2024/1/22
 * @since ：11
 */
@Slf4j
public class BaseDataResultSetHandlerTest {
    protected final ResultSetHandler resultSetHandler;
    protected final Connection connection;
    protected final MetaDataMapper mapper;
    protected final String testDataDir;

    public BaseDataResultSetHandlerTest(String testDataDir, Connection connection, ResultSetHandler resultSetHandler,
        MetaDataMapper mapper) {
        this.testDataDir = testDataDir;
        this.resultSetHandler = resultSetHandler;
        this.connection = connection;
        this.mapper = mapper;
    }

    /**
     * test main
     *
     * @param schema    schema
     * @param tableName tableName
     * @param script    script
     */
    public void testTable(String schema, String tableName, String script) {
        script = testDataDir + "/sql/" + script;
        SqlScriptUtils.execTestSqlScript(connection, script);
        List<String> primaryKeyList = new ArrayList<>();
        Map<String, Map<String, String>> expectResult = testTableExpectResult(schema, tableName, primaryKeyList);
        List<Map<String, String>> result = new ArrayList<>();
        queryTableDataList(tableName, result);
        assertThatData(result, expectResult, primaryKeyList);
    }

    /**
     * testTableExpectResult 测试步骤一: 加载当前表记录预期结果
     *
     * @param schema         schema
     * @param tableName      tableName
     * @param primaryKeyList primaryKeyList
     * @return expectResult
     */
    public Map<String, Map<String, String>> testTableExpectResult(String schema, String tableName,
        List<String> primaryKeyList) {
        primaryKeyList.addAll(initTablePrimaryKeyList(schema, tableName));
        return translateExpectResult(expect(tableName), primaryKeyList);
    }

    /**
     * 加载表预期结果对象
     *
     * @param tableName tableName
     * @return
     */
    public List<Map<String, String>> expect(String tableName) {
        try (InputStream inputStream = new ClassPathResource(
            testDataDir + "/expect/" + tableName + ".json").getInputStream();) {
            return JSONObject.parseObject(IOUtils.toString(inputStream, String.valueOf(StandardCharsets.UTF_8)),
                List.class);
        } catch (IOException ex) {
            throw new ExpectTableDataNotFountException(tableName);
        }
    }

    /**
     * queryTableDataList 测试步骤二 : 查询当前表记录数据
     *
     * @param tableName tableName
     * @param result    result
     */
    public void queryTableDataList(String tableName, List<Map<String, String>> result) {
        String executeQueryStatement = "select * from " + tableName;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            preparedStatement = connection.prepareStatement(executeQueryStatement);
            resultSet = preparedStatement.executeQuery();
            ResultSetMetaData rsmd = resultSet.getMetaData();
            Map<String, String> values = new TreeMap<>();
            while (resultSet.next()) {
                resultSetHandler.putOneResultSetToMap(tableName, rsmd, resultSet, values);
                result.add(new HashMap<>(values));
            }
        } catch (SQLException sqlErr) {
            log.error("test table [{}] error", tableName, sqlErr);
            throw new ExtractJuintTestException("table:" + tableName + " query test data error ");
        } finally {
            close(resultSet);
            close(preparedStatement);
        }
    }

    /**
     * assertThatData 步骤三: 比较数据
     *
     * @param result         query result
     * @param expectResult   expect Result
     * @param primaryKeyList primaryKeyList
     */
    public void assertThatData(List<Map<String, String>> result, Map<String, Map<String, String>> expectResult,
        List<String> primaryKeyList) {
        result.forEach(rowMap -> {
            String primaryKeyValue = getPrimaryKeyValue(primaryKeyList, rowMap);
            Map<String, String> expectMap = expectResult.get(primaryKeyValue);
            assertThat(expectMap, CoreMatchers.notNullValue());
            rowMap.forEach((key, value) -> {
                if (expectMap.containsKey(key)) {
                    assertThat(expectMap, hasEntry(key, value));
                } else {
                    assertThat(value, CoreMatchers.nullValue());
                }
            });
        });
    }

    /**
     * getPrimaryKeyValue
     *
     * @param primaryKeyList primaryKeyList
     * @param rowDataMap     rowDataMap
     * @return value
     */
    public String getPrimaryKeyValue(List<String> primaryKeyList, Map<String, String> rowDataMap) {
        String value;
        if (primaryKeyList.size() == 1) {
            value = rowDataMap.get(primaryKeyList.get(0));
        } else {
            String[] primaryKeyValueBuilder = new String[primaryKeyList.size()];
            for (int i = 0; i < primaryKeyList.size(); i++) {
                primaryKeyValueBuilder[i] = rowDataMap.get(primaryKeyList.get(i));
            }
            value = String.join("_#_", primaryKeyValueBuilder);
        }
        return value;
    }

    private List<String> initTablePrimaryKeyList(String schema, String tableName) {
        BiFunction<String, String, List<PrimaryColumnBean>> function = mapper::queryTablePrimaryColumnsByTableName;
        return initTablePrimaryKeyList(schema, tableName, function);
    }

    private Map<String, Map<String, String>> translateExpectResult(List<Map<String, String>> expectResultList,
        List<String> primaryKeyList) {
        Map<String, Map<String, String>> expectMap = new HashMap<>();
        if (CollectionUtils.isEmpty(expectResultList)) {
            return expectMap;
        }
        expectResultList.forEach(expectRowMap -> {
            expectMap.put(getPrimaryKeyValue(primaryKeyList, expectRowMap), expectRowMap);
        });
        return expectMap;
    }

    private List<String> initTablePrimaryKeyList(String schema, String tableName,
        BiFunction<String, String, List<PrimaryColumnBean>> function) {
        List<PrimaryColumnBean> primaryKeys = function.apply(schema, tableName);
        return primaryKeys.stream()
                          .map(PrimaryColumnBean::getColumnName)
                          .collect(Collectors.toList());
    }

    /**
     * 关闭测试结果集
     *
     * @param resultSet resultSet
     */
    public void close(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException sql) {
            }
        }
    }

    /**
     * 关闭测试 preparedStatement
     *
     * @param preparedStatement preparedStatement
     */
    public void close(PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException sql) {
            }
        }
    }
}
