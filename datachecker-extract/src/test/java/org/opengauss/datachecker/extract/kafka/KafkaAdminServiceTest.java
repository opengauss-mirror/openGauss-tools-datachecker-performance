package org.opengauss.datachecker.extract.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.extract.boot.SpringBootStartTest;
import org.opengauss.datachecker.extract.boot.TestSourceActiveProfiles;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaAdminServiceTest implements SpringBootStartTest, TestSourceActiveProfiles {

    private KafkaAdminService kafkaAdminServiceUnderTest;

    @BeforeEach
    void setUp() {
        kafkaAdminServiceUnderTest = new KafkaAdminService();
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
        assertThat(result).contains("topic");
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
