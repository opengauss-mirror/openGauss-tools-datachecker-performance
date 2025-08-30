package org.opengauss.datachecker.check.util;

import com.alibaba.fastjson.JSONObject;

import org.apache.logging.log4j.core.util.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author ：wangchao
 * @date ：Created in 2022/12/27
 * @since ：11
 */
public class TestJsonUtil {
    public static final String KEY_UPDATE_DML_BUILDER_TEST = "update_dml_builder_test";
    public static final String KEY_META_DATA_13_TABLE = "meta_data_13_table";
    public static final String KEY_DEBEZIUM_ONE_TABLE_RECORD = "debezium_one_table_record";

    private static final String RESOURCE_META_JSON = "data/update_dml_builder_test/metadata.json";
    private static final String RESOURCE_DEBEZIUM_ONE_TABLE_RECORD = "data/debezium_data/one_table_record.json";
    private static final String RESOURCE_META_DATA_13_TABLE = "data/meta_data/metadata_table_13.json";

    private static final Map<String, String> JSON_RESOURCE = new HashMap<>();

    static {
        JSON_RESOURCE.put(KEY_UPDATE_DML_BUILDER_TEST, RESOURCE_META_JSON);
        JSON_RESOURCE.put(KEY_META_DATA_13_TABLE, RESOURCE_META_DATA_13_TABLE);
        JSON_RESOURCE.put(KEY_DEBEZIUM_ONE_TABLE_RECORD, RESOURCE_DEBEZIUM_ONE_TABLE_RECORD);
    }

    public static String getJsonText(String key) {
        return mockDataJson(JSON_RESOURCE.get(key));
    }

    private static String mockDataJson(String resource) {
        try (InputStream inputStream = TestJsonUtil.class.getClassLoader().getResourceAsStream(resource)) {
            if (Objects.nonNull(inputStream)) {
                return IOUtils.toString(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            } else {
                return null;
            }
        } catch (IOException ex) {
            return null;
        }
    }

    public static <T> T parseObject(String key, Class<T> classz) {
        return JSONObject.parseObject(mockDataJson(JSON_RESOURCE.get(key)), classz);
    }

    public static <T> HashMap<String, T> parseHashMap(String key, Class<T> classz) {
        final Map<String, JSONObject> temp =
            JSONObject.parseObject(mockDataJson(JSON_RESOURCE.get(key)), HashMap.class);
        final HashMap<String, T> result = new HashMap<>();
        temp.entrySet().forEach(entry -> {
            result.put(entry.getKey(), JSONObject.toJavaObject(entry.getValue(), classz));
        });
        return result;
    }
}
