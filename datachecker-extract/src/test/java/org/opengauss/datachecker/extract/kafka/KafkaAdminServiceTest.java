package org.opengauss.datachecker.extract.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaAdminServiceTest {

    private KafkaAdminService kafkaAdminServiceUnderTest;

    @BeforeEach
    void setUp() {
        kafkaAdminServiceUnderTest = new KafkaAdminService();
        ConfigCache.put(ConfigConstants.KAFKA_SERVERS, "192.168.0.114:9092");
        kafkaAdminServiceUnderTest.initAdminClient();
    }

    @Test
    void testInitAdminClient() {
        // Setup
        // Run the test
        kafkaAdminServiceUnderTest.initAdminClient();

        // Verify the results
    }

    @Test
    void testCreateTopic() {
        // Setup
        // Run the test
        final boolean result = kafkaAdminServiceUnderTest.createTopic("topic", 1);

        // Verify the results
        assertThat(result).isTrue();
    }

    @Test
    void testDeleteTopic() {
        // Setup
        // Run the test
        kafkaAdminServiceUnderTest.deleteTopic(List.of("topic"));

        // Verify the results
    }

    @Test
    void testGetAllTopic1() {
        // Setup
        // Run the test
        final List<String> result = kafkaAdminServiceUnderTest.getAllTopic("topic");

        // Verify the results
        assertThat(result).isEqualTo(List.of("topic"));
    }

    @Test
    void testGetAllTopic2() {
        // Setup
        // Run the test
        final List<String> result = kafkaAdminServiceUnderTest.getAllTopic();

        // Verify the results
        assertThat(result).isEqualTo(List.of("topic"));
    }

    @Test
    void testIsTopicExists() {
        // Setup
        // Run the test
        final boolean result = kafkaAdminServiceUnderTest.isTopicExists("topic");

        // Verify the results
        assertThat(result).isTrue();
    }

    @Test
    void testCloseAdminClient() {
        // Setup
        // Run the test
        kafkaAdminServiceUnderTest.closeAdminClient();

        // Verify the results
    }
}
