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

package org.opengauss.datachecker.extract.task;

/**
 * OpenGaussCsvResultSetHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/19
 * @since ：11
 */
public class OpenGaussCsvResultSetHandler extends OpenGaussResultSetHandler {
    {
        simpleTypeHandlers.put(OpenGaussType.BIT, typeHandlerFactory.createCsvBitHandler());
        // csv 文件解析浮点类型数据结果为gsql 原值
        simpleTypeHandlers.put(OpenGaussType.FLOAT1, typeHandlerFactory.createCsvFloatHandler());
        simpleTypeHandlers.put(OpenGaussType.FLOAT2, typeHandlerFactory.createCsvFloatHandler());
        simpleTypeHandlers.put(OpenGaussType.FLOAT4, typeHandlerFactory.createCsvFloatHandler());
        simpleTypeHandlers.put(OpenGaussType.FLOAT8, typeHandlerFactory.createCsvFloatHandler());
        simpleTypeHandlers.put(OpenGaussType.NUMERIC, typeHandlerFactory.createCsvFloatHandler());
        openGaussTypeHandlers.remove(OpenGaussType.FLOAT1);
        openGaussTypeHandlers.remove(OpenGaussType.FLOAT2);
        openGaussTypeHandlers.remove(OpenGaussType.FLOAT4);
        openGaussTypeHandlers.remove(OpenGaussType.FLOAT8);
        commonTypeHandlers.remove(OpenGaussType.NUMERIC);
    }
}
