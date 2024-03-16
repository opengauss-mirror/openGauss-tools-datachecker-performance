package org.opengauss.datachecker.check.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.util.TestJsonUtil;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.PageExtract;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.exception.CheckMetaDataException;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EndpointMetaDataManagerTest {

    @Mock
    private EndpointStatusManager mockEndpointStatusManager;
    @Mock
    private FeignClientService mockFeignClientService;
    EndpointMetaDataManager endpointMetaDataManagerUnderTest;

    @BeforeEach
    void setUp() {
        endpointMetaDataManagerUnderTest =
            new EndpointMetaDataManager(mockEndpointStatusManager, mockFeignClientService);
        endpointMetaDataManagerUnderTest.clearCache();
    }

    @DisplayName("load fail all no")
    @Test
    void testLoad_fail_all_no() {
        PageExtract pageExtract = PageExtract.buildInitPage(0);
        when(mockFeignClientService.queryMetaDataOfSchema(Endpoint.SOURCE, pageExtract)).thenReturn(new HashMap<>());
        when(mockFeignClientService.queryMetaDataOfSchema(Endpoint.SINK, pageExtract)).thenReturn(new HashMap<>());
        endpointMetaDataManagerUnderTest.isMetaLoading();
        assertThatThrownBy(() -> endpointMetaDataManagerUnderTest.load()).isInstanceOf(CheckMetaDataException.class);
    }

    @DisplayName("load fail no source")
    @Test
    void testLoad_fail_no_source() {
        PageExtract pageExtract = PageExtract.buildInitPage(13);
        final HashMap<String, TableMetadata> metadataMap =
            TestJsonUtil.parseHashMap(TestJsonUtil.KEY_META_DATA_13_TABLE, TableMetadata.class);
        when(mockFeignClientService.queryMetaDataOfSchema(Endpoint.SOURCE, pageExtract)).thenReturn(new HashMap<>());
        when(mockFeignClientService.queryMetaDataOfSchema(Endpoint.SINK, pageExtract)).thenReturn(metadataMap);
        endpointMetaDataManagerUnderTest.isMetaLoading();
        // Setup
        // Run the test
        // Verify the results
        assertThatThrownBy(() -> endpointMetaDataManagerUnderTest.load()).isInstanceOf(CheckMetaDataException.class);
    }

    @DisplayName("load source and sink success")
    @Test
    void testLoad_success() {
        PageExtract pageExtract = PageExtract.buildInitPage(13);
        final HashMap<String, TableMetadata> metadataMap =
            TestJsonUtil.parseHashMap(TestJsonUtil.KEY_META_DATA_13_TABLE, TableMetadata.class);
        when(mockFeignClientService.queryMetaDataOfSchema(Endpoint.SOURCE, pageExtract)).thenReturn(metadataMap);
        when(mockFeignClientService.queryMetaDataOfSchema(Endpoint.SINK, pageExtract)).thenReturn(metadataMap);
        endpointMetaDataManagerUnderTest.isMetaLoading();
        // Setup
        // Run the test
        endpointMetaDataManagerUnderTest.load();
        // Verify the results
    }

    @DisplayName("meta load and return load status")
    @Test
    void testIsMetaLoading() {
        PageExtract pageExtract = PageExtract.buildInitPage(13);
        final HashMap<String, TableMetadata> metadataMap =
            TestJsonUtil.parseHashMap(TestJsonUtil.KEY_META_DATA_13_TABLE, TableMetadata.class);
        when(mockFeignClientService.queryMetaDataOfSchema(Endpoint.SOURCE, pageExtract)).thenReturn(metadataMap);
        when(mockFeignClientService.queryMetaDataOfSchema(Endpoint.SINK, pageExtract)).thenReturn(metadataMap);
        // Run the test
        final boolean result = endpointMetaDataManagerUnderTest.isMetaLoading();
        // Verify the results
        assertThat(result).isFalse();
    }

    @Test
    void testGetTableMetadata() {
        PageExtract pageExtract = PageExtract.buildInitPage(13);
        final HashMap<String, TableMetadata> metadataMap =
            TestJsonUtil.parseHashMap(TestJsonUtil.KEY_META_DATA_13_TABLE, TableMetadata.class);
        when(mockFeignClientService.queryMetaDataOfSchema(Endpoint.SOURCE, pageExtract)).thenReturn(metadataMap);
        when(mockFeignClientService.queryMetaDataOfSchema(Endpoint.SINK, pageExtract)).thenReturn(metadataMap);
        endpointMetaDataManagerUnderTest.isMetaLoading();
        // Run the test
        final TableMetadata result =
            endpointMetaDataManagerUnderTest.getTableMetadata(Endpoint.SOURCE, "t_data_checker_0033_02");
        // Verify the results
        assertThat(result).isEqualTo(metadataMap.get("t_data_checker_0033_02"));
    }

    @DisplayName("endpoint health ")
    @Test
    void testIsEndpointHealth() {
        when(mockEndpointStatusManager.isEndpointHealth()).thenReturn(false);
        // Run the test
        final boolean result = endpointMetaDataManagerUnderTest.isEndpointHealth();
        // Verify the results
        assertThat(result).isFalse();
    }

    @DisplayName("get check table list success")
    @Test
    void testGetCheckTableList() {
        PageExtract pageExtract = PageExtract.buildInitPage(13);
        final HashMap<String, TableMetadata> metadataMap =
            TestJsonUtil.parseHashMap(TestJsonUtil.KEY_META_DATA_13_TABLE, TableMetadata.class);
        when(mockFeignClientService.queryMetaDataOfSchema(Endpoint.SOURCE, pageExtract)).thenReturn(metadataMap);
        when(mockFeignClientService.queryMetaDataOfSchema(Endpoint.SINK, pageExtract)).thenReturn(metadataMap);
        endpointMetaDataManagerUnderTest.isMetaLoading();
        // Setup
        // Run the test
        endpointMetaDataManagerUnderTest.load();
        List<String> result = endpointMetaDataManagerUnderTest.getCheckTableList();
        result.sort(Comparator.naturalOrder());
        assertThat(result).isEqualTo(metadataMap.keySet()
                                                .stream()
                                                .sorted()
                                                .collect(Collectors.toList()));
    }

    @DisplayName("get check miss table list empty")
    @Test
    void testGetMissTableList() {
        assertThat(endpointMetaDataManagerUnderTest.getMissTableList()).isEmpty();
    }
}
