package org.opengauss.datachecker.check.modules.check;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.check.config.KafkaConsumerConfig;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.util.IdGenerator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerHandlerTest {

    @Mock
    private KafkaConsumer<String, String> mockConsumer;

    private KafkaConsumerHandler kafkaConsumerHandlerUnderTest;

    private void initKafka() {
        ConfigCache.put(ConfigConstants.KAFKA_SERVERS, "192.168.0.114:9092");
        ConfigCache.put(ConfigConstants.KAFKA_AUTO_COMMIT, true);
        ConfigCache.put(ConfigConstants.KAFKA_DEFAULT_GROUP_ID, "groupId");
        ConfigCache.put(ConfigConstants.KAFKA_AUTO_OFFSET_RESET, "earliest");
        ConfigCache.put(ConfigConstants.KAFKA_MAX_POLL_RECORDS, 20000);
        ConfigCache.put(ConfigConstants.KAFKA_FETCH_MAX_BYTES, 536870912);
        ConfigCache.put(ConfigConstants.KAFKA_REQUEST_TIMEOUT, 300000);
    }

    @BeforeEach
    void setUp() {
        initKafka();
        KafkaConsumerConfig kafkaConsumerConfig = new KafkaConsumerConfig();
        mockConsumer = buildKafkaConsumer(kafkaConsumerConfig, true);
        kafkaConsumerHandlerUnderTest = new KafkaConsumerHandler(mockConsumer, 0);
    }

    public KafkaConsumer<String, String> buildKafkaConsumer(KafkaConsumerConfig config, boolean isNewGroup) {
        Consumer<String, String> consumer;
        if (isNewGroup) {
            consumer = config.consumerFactory(IdGenerator.nextId36()).createConsumer();
        } else {
            consumer = config.consumerFactory().createConsumer();
        }
        return (KafkaConsumer<String, String>) consumer;
    }

    @AfterEach
    void setUp2() {
        mockConsumer.unsubscribe();
        mockConsumer.close();
    }

    @Test
    public void pollTpSliceData() {
        TopicPartition topicPartition = new TopicPartition("CHECK_18LF6JWR5N9C_1_sbtest_50w_1_0", 0);
        List<RowDataHash> dataList = new ArrayList<>();
        mockConsumer.assign(List.of(topicPartition));
        mockConsumer.seek(topicPartition, 0);

        AtomicLong currentCount = new AtomicLong(0);
        while (currentCount.get() < 1000) {
            ConsumerRecords<String, String> records = mockConsumer.poll(Duration.ofMillis(100));
            if (records.count() > 0) {
                records.forEach(record -> {
                    RowDataHash row = JSON.parseObject(record.value(), RowDataHash.class);
                    if (row.getSNo() == 1) {
                        dataList.add(row);
                        currentCount.incrementAndGet();
                    }
                });
            }
        }
        System.out.println(currentCount.get());
    }

    @Test
    void testConsumer() {
        TopicPartition topicPartition = new TopicPartition("CHECK_18L3VIL4I4N4_1_sbtest_10w_1_0", 0);
        mockConsumer.assign(List.of(topicPartition));
        //        kafkaConsumer.seek(topicPartition, sExtend.getStartOffset());
        long begin = beginningOffsets(topicPartition);
        long end = getEndOfOffset(topicPartition);
        mockConsumer.seek(topicPartition, 0);
        long recordSize = end - begin;
        if (recordSize > 0) {
            while (true) {
                ConsumerRecords<String, String> records = mockConsumer.poll(Duration.ofMillis(10));
                records.forEach(record -> {
                    System.out.println(record);
                });
                System.out.println(records.count());

            }
        }

    }

    private long getEndOfOffset(TopicPartition topicPartition) {
        final Map<TopicPartition, Long> topicPartitionLongMap = mockConsumer.endOffsets(List.of(topicPartition));
        return topicPartitionLongMap.get(topicPartition);
    }

    private long beginningOffsets(TopicPartition topicPartition) {
        final Map<TopicPartition, Long> topicPartitionLongMap = mockConsumer.beginningOffsets(List.of(topicPartition));
        return topicPartitionLongMap.get(topicPartition);
    }

    @Test
    void testConsumerSubscribe() {
        mockConsumer.subscribe(List.of("CHECK_18L3VIL4I4N4_1_sbtest_10w_1_0"));
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            mockConsumer.poll(Duration.ofMillis(10));
            assignment = mockConsumer.assignment();
        }
        for (TopicPartition tp : assignment) {
            mockConsumer.seek(tp, 100);
        }
        while (true) {
            ConsumerRecords<String, String> consumerRecords = mockConsumer.poll(1000L);
            Set<TopicPartition> partitions = consumerRecords.partitions();
            for (TopicPartition tp : partitions) {
                List<ConsumerRecord<String, String>> records = consumerRecords.records(tp);
                for (ConsumerRecord<String, String> record : records) {
                    //process record
                    System.out.println(record);
                }
                long lastConsumedOffset = records.get(records.size() - 1).offset();
                //保存位移
                if (records.size() > 0) {
                    break;
                }
            }
        }
    }

    @Test
    void testQueryCheckRowData() {
        // Setup
        final RowDataHash rowDataHash = new RowDataHash();
        rowDataHash.setKey("key");
        rowDataHash.setIdx(0);
        rowDataHash.setKHash(0L);
        rowDataHash.setVHash(0L);
        rowDataHash.setSNo(0);
        final List<RowDataHash> expectedResult = List.of(rowDataHash);

        // Configure KafkaConsumer.endOffsets(...).
        final Map<TopicPartition, Long> topicPartitionLongMap =
            Map.ofEntries(Map.entry(new TopicPartition("topic", 0), 0L));
        when(mockConsumer.endOffsets(List.of(new TopicPartition("topic", 0)))).thenReturn(topicPartitionLongMap);

        // Configure KafkaConsumer.beginningOffsets(...).
        final Map<TopicPartition, Long> topicPartitionLongMap1 =
            Map.ofEntries(Map.entry(new TopicPartition("topic", 0), 0L));
        when(mockConsumer.beginningOffsets(List.of(new TopicPartition("topic", 0)))).thenReturn(topicPartitionLongMap1);

        // Run the test
        final List<RowDataHash> result = kafkaConsumerHandlerUnderTest.queryCheckRowData("topic", 0);

        // Verify the results
        assertThat(result).isEqualTo(expectedResult);
        verify(mockConsumer).assign(List.of(new TopicPartition("topic", 0)));
    }

    @Test
    void testPoolTopicPartitionsData() {
        // Setup
        final RowDataHash rowDataHash = new RowDataHash();
        rowDataHash.setKey("key");
        rowDataHash.setIdx(0);
        rowDataHash.setKHash(0L);
        rowDataHash.setVHash(0L);
        rowDataHash.setSNo(0);
        final List<RowDataHash> list = List.of(rowDataHash);

        // Configure KafkaConsumer.endOffsets(...).
        final Map<TopicPartition, Long> topicPartitionLongMap =
            Map.ofEntries(Map.entry(new TopicPartition("topic", 0), 0L));
        when(mockConsumer.endOffsets(List.of(new TopicPartition("topic", 0)))).thenReturn(topicPartitionLongMap);

        // Configure KafkaConsumer.beginningOffsets(...).
        final Map<TopicPartition, Long> topicPartitionLongMap1 =
            Map.ofEntries(Map.entry(new TopicPartition("topic", 0), 0L));
        when(mockConsumer.beginningOffsets(List.of(new TopicPartition("topic", 0)))).thenReturn(topicPartitionLongMap1);

        // Run the test
        kafkaConsumerHandlerUnderTest.poolTopicPartitionsData("topic", 0, list);

        // Verify the results
        verify(mockConsumer).assign(List.of(new TopicPartition("topic", 0)));
    }

    @Test
    void testPollTpSliceData() {
        // Setup
        final TopicPartition topicPartition = new TopicPartition("topic", 0);
        final SliceExtend sExtend = new SliceExtend();
        sExtend.setEndpoint(Endpoint.SOURCE);
        sExtend.setName("name");
        sExtend.setNo(0);
        sExtend.setStartOffset(0L);
        sExtend.setEndOffset(0L);
        sExtend.setCount(0);
        sExtend.setStatus(0);
        sExtend.setTableHash(0L);

        final RowDataHash rowDataHash = new RowDataHash();
        rowDataHash.setKey("key");
        rowDataHash.setIdx(0);
        rowDataHash.setKHash(0L);
        rowDataHash.setVHash(0L);
        rowDataHash.setSNo(0);
        final List<RowDataHash> dataList = List.of(rowDataHash);

        // Configure KafkaConsumer.poll(...).
        final ConsumerRecords<String, String> consumerRecords = new ConsumerRecords<>(Map.ofEntries(
            Map.entry(new TopicPartition("topic", 0), List.of(new ConsumerRecord<>("topic", 0, 0L, "key", "value")))));
        when(mockConsumer.poll(Duration.ofDays(0L))).thenReturn(consumerRecords);

        // Run the test
        kafkaConsumerHandlerUnderTest.pollTpSliceData(topicPartition, sExtend, dataList);

        // Verify the results
        verify(mockConsumer).assign(List.of(new TopicPartition("topic", 0)));
        verify(mockConsumer).seek(new TopicPartition("topic", 0), 0L);
    }

    @Test
    void testPollTpSliceData_KafkaConsumerPollReturnsNoItems() {
        // Setup
        final TopicPartition topicPartition = new TopicPartition("topic", 0);
        final SliceExtend sExtend = new SliceExtend();
        sExtend.setEndpoint(Endpoint.SOURCE);
        sExtend.setName("name");
        sExtend.setNo(0);
        sExtend.setStartOffset(0L);
        sExtend.setEndOffset(0L);
        sExtend.setCount(0);
        sExtend.setStatus(0);
        sExtend.setTableHash(0L);

        final RowDataHash rowDataHash = new RowDataHash();
        rowDataHash.setKey("key");
        rowDataHash.setIdx(0);
        rowDataHash.setKHash(0L);
        rowDataHash.setVHash(0L);
        rowDataHash.setSNo(0);
        final List<RowDataHash> dataList = List.of(rowDataHash);
        when(mockConsumer.poll(Duration.ofDays(0L))).thenReturn(ConsumerRecords.empty());

        // Run the test
        kafkaConsumerHandlerUnderTest.pollTpSliceData(topicPartition, sExtend, dataList);

        // Verify the results
        verify(mockConsumer).assign(List.of(new TopicPartition("topic", 0)));
        verify(mockConsumer).seek(new TopicPartition("topic", 0), 0L);
    }

    @Test
    void testQueryRowData() {
        // Setup
        final RowDataHash rowDataHash = new RowDataHash();
        rowDataHash.setKey("key");
        rowDataHash.setIdx(0);
        rowDataHash.setKHash(0L);
        rowDataHash.setVHash(0L);
        rowDataHash.setSNo(0);
        final List<RowDataHash> expectedResult = List.of(rowDataHash);

        // Configure KafkaConsumer.endOffsets(...).
        final Map<TopicPartition, Long> topicPartitionLongMap =
            Map.ofEntries(Map.entry(new TopicPartition("topic", 0), 0L));
        when(mockConsumer.endOffsets(List.of(new TopicPartition("topic", 0)))).thenReturn(topicPartitionLongMap);

        // Configure KafkaConsumer.beginningOffsets(...).
        final Map<TopicPartition, Long> topicPartitionLongMap1 =
            Map.ofEntries(Map.entry(new TopicPartition("topic", 0), 0L));
        when(mockConsumer.beginningOffsets(List.of(new TopicPartition("topic", 0)))).thenReturn(topicPartitionLongMap1);

        // Run the test
        final List<RowDataHash> result = kafkaConsumerHandlerUnderTest.queryRowData("topic", 0, false);

        // Verify the results
        assertThat(result).isEqualTo(expectedResult);
        verify(mockConsumer).assign(List.of(new TopicPartition("topic", 0)));
    }

    @Test
    void testCloseConsumer() {
        // Setup
        // Run the test
        kafkaConsumerHandlerUnderTest.closeConsumer();

        // Verify the results
        verify(mockConsumer).close(Duration.ofDays(0L));
    }
}
