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

import javax.validation.constraints.AssertTrue;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

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

    @Test
    void testLineCsvFileReader() throws IOException, CsvValidationException {
        URL resource = TestJsonUtil.class.getClassLoader().getResource(
            TestJsonUtil.getFileName(TestJsonUtil.KEY_DATA_TEST_DEMO_SLICE1_CSV));
        CSVReader reader = new CSVReader(new BufferedReader(new FileReader(resource.getFile())));
        reader.skip(3);
        assertEquals("[4, storage, aliyun, {\"name\":\"阿里云存储\",\"bucket\":\"\",\"secretKey\":\"\",\"accessKey\":\"\",\"domain\":\"\"}, 1660620367, 1662620071]",Arrays.toString(reader.peek()));
        reader.skip(3);
        assertEquals("[7, sms, aliyun, {\"name\":\"阿里云短信\",\"alias\":\"aliyun\",\"sign\":\"\",\"appKey\":\"\",\"secretKey\":\"\"}, 1660620367, 1660620367]", Arrays.toString(reader.peek()));
        reader.close();
    }
}
