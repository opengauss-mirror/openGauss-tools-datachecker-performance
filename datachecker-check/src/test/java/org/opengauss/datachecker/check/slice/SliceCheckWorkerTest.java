package org.opengauss.datachecker.check.slice;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.check.config.KafkaConsumerConfig;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.util.IdGenerator;
import org.springframework.kafka.core.ConsumerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


@ExtendWith(MockitoExtension.class)
class SliceCheckWorkerTest {

    private KafkaConsumerConfig kafkaConsumerConfig;

    @BeforeEach
    void setUp() {
        init();
        kafkaConsumerConfig = new KafkaConsumerConfig();
    }

    public void init() {
        ConfigCache.put(ConfigConstants.PROCESS_NO, IdGenerator.nextId36());
        ConfigCache.put(ConfigConstants.KAFKA_SERVERS, "127.0.0.1:9092");
        ConfigCache.put(ConfigConstants.KAFKA_DEFAULT_GROUP_ID, ConfigCache.getValue(ConfigConstants.PROCESS_NO));
        ConfigCache.put(ConfigConstants.KAFKA_AUTO_COMMIT, true);
        ConfigCache.put(ConfigConstants.KAFKA_AUTO_OFFSET_RESET, true);
        ConfigCache.put(ConfigConstants.KAFKA_MAX_POLL_RECORDS, 1000);
        ConfigCache.put(ConfigConstants.KAFKA_FETCH_MAX_BYTES, 50000);
        ConfigCache.put(ConfigConstants.KAFKA_REQUEST_TIMEOUT, 1000);
    }

    @Test
    void testRun() {
        ConsumerFactory<String, String> consumerFactory =
            kafkaConsumerConfig.consumerFactory(ConfigCache.getValue(ConfigConstants.PROCESS_NO));
        Consumer<String, String> kafkaConsumer = consumerFactory.createConsumer();

        TopicPartition topicPartition = new TopicPartition("CHECK_1AKFAA1H143K_2_sbtest_10w_1_0-0", 0);
        kafkaConsumer.assign(List.of(topicPartition));
        SliceExtend sExtend = new SliceExtend();
        sExtend.setNo(1);
        kafkaConsumer.seek(topicPartition, sExtend.getStartOffset());
        AtomicLong currentCount = new AtomicLong(0);
        while (currentCount.get() < sExtend.getCount()) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(200));
            if (records.count() <= 0) {
                continue;
            }
            records.forEach(record -> {
                RowDataHash row = JSON.parseObject(record.value(), RowDataHash.class);
                if (row.getSNo() == sExtend.getNo()) {

                }
            });
        }
    }
}
