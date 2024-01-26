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

package org.opengauss.datachecker.extract.slice.process;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.extract.slice.SliceProcessorContext;
import org.opengauss.datachecker.extract.util.TestJsonUtil;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;

@ExtendWith(MockitoExtension.class)
class CsvSliceProcessorTest {

    @Mock
    private SliceVo mockSlice;
    @Mock
    private SliceProcessorContext mockContext;

    private CsvSliceProcessor csvSliceProcessorUnderTest;

    @BeforeEach
    void setUp() {
        csvSliceProcessorUnderTest = new CsvSliceProcessor(mockSlice, mockContext);
    }

    @Test
    void testCsvFileReader() throws IOException, CsvValidationException {
        URL resource = TestJsonUtil.class.getClassLoader().getResource(
            TestJsonUtil.getFileName(TestJsonUtil.KEY_DATA_TEST_DEMO_SLICE1_CSV));
        CSVReader reader = new CSVReader(new BufferedReader(new FileReader(resource.getFile())));
        String[] readLine;
        while ((readLine = reader.readNext()) != null) {
            System.out.println(Arrays.toString(readLine));
        }
        reader.close();
    }
}
