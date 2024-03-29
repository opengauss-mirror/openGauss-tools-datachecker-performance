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

package org.opengauss.datachecker.check.modules.check;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.cache.CheckRateCache;
import org.opengauss.datachecker.check.cache.TableStatusRegister;
import org.opengauss.datachecker.check.modules.bucket.Bucket;
import org.opengauss.datachecker.check.modules.bucket.BuilderBucketHandler;
import org.opengauss.datachecker.check.modules.bucket.CheckTuple;
import org.opengauss.datachecker.check.modules.merkle.MerkleTree;
import org.opengauss.datachecker.check.modules.merkle.MerkleTree.Node;
import org.opengauss.datachecker.check.modules.report.CheckResultManagerService;
import org.opengauss.datachecker.check.service.EndpointMetaDataManager;
import org.opengauss.datachecker.check.service.StatisticalService;
import org.opengauss.datachecker.common.constant.Constants;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.opengauss.datachecker.common.entry.check.CheckPartition;
import org.opengauss.datachecker.common.entry.check.CheckTable;
import org.opengauss.datachecker.common.entry.check.DataCheckParam;
import org.opengauss.datachecker.common.entry.check.DifferencePair;
import org.opengauss.datachecker.common.entry.check.Pair;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ConditionLimit;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.exception.LargeDataDiffException;
import org.opengauss.datachecker.common.exception.MerkleTreeDepthException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.SpringUtil;
import org.opengauss.datachecker.common.util.TopicUtil;
import org.springframework.lang.NonNull;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * DataCheckRunnable
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class DataCheckRunnable implements Runnable {
    private static final Logger log = LogUtils.getCheckLogger();
    private static final Logger logKafka = LogUtils.geKafkaLogger();
    private static final int THRESHOLD_MIN_BUCKET_SIZE = 2;

    private final DifferencePair<Map<String, RowDataHash>, Map<String, RowDataHash>, Map<String, Pair<Node, Node>>>
        difference = DifferencePair.of(new HashMap<>(), new HashMap<>(), new HashMap<>());
    private final StatisticalService statisticalService;
    private final TableStatusRegister tableStatusRegister;
    private final DataCheckParam checkParam;
    private final KafkaConsumerService kafkaConsumerService;
    private final EndpointMetaDataManager endpointMetaDataManager;
    private final CheckResultManagerService checkResultManagerService;
    private String sinkSchema;
    private String sourceTopic;
    private String sinkTopic;
    private String tableName;
    private int partitions;
    private int rowCount;
    private int tablePartitionRowCount;
    private int errorRate;
    private int bucketCapacity;
    private LocalDateTime startTime;
    private CheckPartition checkPartition;
    private CheckRateCache checkRateCache;

    /**
     * DataCheckRunnable
     *
     * @param checkParam checkParam
     * @param support    support
     */
    public DataCheckRunnable(@NonNull DataCheckParam checkParam, @NonNull DataCheckRunnableSupport support) {
        this.checkParam = checkParam;
        tableName = checkParam.getTableName();
        startTime = LocalDateTime.now();
        statisticalService = support.getStatisticalService();
        tableStatusRegister = support.getTableStatusRegister();
        checkResultManagerService = support.getCheckResultManagerService();
        kafkaConsumerService = support.getKafkaConsumerService();
        checkRateCache = SpringUtil.getBean(CheckRateCache.class);
        endpointMetaDataManager = SpringUtil.getBean(EndpointMetaDataManager.class);
    }

    private void paramInit() {
        tableName = checkParam.getTableName();
        partitions = checkParam.getPartitions();
        sourceTopic = TopicUtil.buildTopicName(checkParam.getProcess(), Endpoint.SOURCE, tableName);
        sinkTopic = TopicUtil.buildTopicName(checkParam.getProcess(), Endpoint.SINK, tableName);
        sinkSchema = checkParam.getSchema();
        rowCount = 0;
        tablePartitionRowCount = checkParam.getTablePartitionRowCount();
        errorRate = checkParam.getErrorRate();
        bucketCapacity = checkParam.getBucketCapacity();
        resetThreadName(tableName, partitions);
        checkPartition = new CheckPartition(tableName, partitions);
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        try {
            paramInit();
            log.info("check table [{},{}] start", tableName, partitions);
            checkTableData();
        } catch (Exception ignore) {
            log.error("check table has some error,", ignore);
        } finally {
            statisticalService.statistics(getStatisticsName(tableName, partitions), startTime);
            refreshCheckStatus();
            checkResult();
            cleanCheckThreadEnvironment();
            checkRateCache.add(buildCheckTable());
            log.info("check table result [{},{}] complete!", tableName, partitions);
        }
    }

    private void checkTableData() throws InterruptedException {
        CheckTuple sourceTuple = CheckTuple.of(Endpoint.SOURCE, partitions, new LinkedList<>());
        CheckTuple sinkTuple = CheckTuple.of(Endpoint.SINK, partitions, new LinkedList<>());
        // Initialize bucket list
        initBucketList(sourceTuple, sinkTuple);
        // No Merkel tree verification algorithm scenario
        if (!shouldCheckMerkleTree(sourceTuple.getBucketSize(), sinkTuple.getBucketSize())) {
            compareNoMerkleTree(sourceTuple, sinkTuple);
        } else {
            // Construct Merkel tree constraint: bucketList cannot be empty, and size > =2
            MerkleTree sourceTree = new MerkleTree(sourceTuple.getBuckets());
            MerkleTree sinkTree = new MerkleTree(sinkTuple.getBuckets());
            // Merkel tree comparison
            if (sourceTree.getDepth() != sinkTree.getDepth()) {
                refreshCheckStatus();
                throw new MerkleTreeDepthException(String.format(Locale.ROOT,
                    "source & sink data have large different, Please synchronize data again! "
                        + "merkel tree depth different,source depth=[%d],sink depth=[%d]", sourceTree.getDepth(),
                    sinkTree.getDepth()));
            }
            // Recursively compare two Merkel trees and return the difference record.
            compareMerkleTree(sourceTree, sinkTree);
        }
    }

    /**
     * Initialize bucket list
     */
    private void initBucketList(CheckTuple sourceTuple, CheckTuple sinkTuple) throws InterruptedException {
        List<CheckTuple> checkTupleList = List.of(sourceTuple, sinkTuple);
        Map<Integer, Pair<Integer, Integer>> bucketDiff = new ConcurrentHashMap<>();
        // Get the Kafka partition number corresponding to the current task
        // Initialize source bucket column list data
        CountDownLatch countDownLatch = new CountDownLatch(checkTupleList.size());
        checkTupleList.parallelStream().forEach(check -> {
            initBucketList(check.getEndpoint(), check.getPartitionNo(), check.getBuckets(), bucketDiff);
            countDownLatch.countDown();
        });
        countDownLatch.await();

        // Align the source and destination bucket list
        alignAllBuckets(sourceTuple, sinkTuple, bucketDiff);
        sortBuckets(sourceTuple.getBuckets());
        sortBuckets(sinkTuple.getBuckets());
        log.debug("initialize bucket construction is currently completed of table [{}-{}]", tableName, partitions);
    }

    /**
     * Sort the final bucket list by bucket number
     *
     * @param bucketList bucketList
     */
    private void sortBuckets(@NonNull List<Bucket> bucketList) {
        bucketList.sort(Comparator.comparingInt(Bucket::getNumber));
    }

    /**
     * <pre>
     * Align the bucket list data according to the statistical results of source
     * and destination bucket difference information {@code bucketNumberDiffMap}.
     * </pre>
     */
    private void alignAllBuckets(CheckTuple sourceTuple, CheckTuple sinkTuple,
        Map<Integer, Pair<Integer, Integer>> bucketDiff) {
        if (MapUtils.isNotEmpty(bucketDiff)) {
            bucketDiff.forEach((number, pair) -> {
                if (pair.getSource() == -1) {
                    sourceTuple.getBuckets().add(BuilderBucketHandler.builderEmpty(number));
                }
                if (pair.getSink() == -1) {
                    sinkTuple.getBuckets().add(BuilderBucketHandler.builderEmpty(number));
                }
            });
        }
    }

    private void initBucketList(Endpoint endpoint, int partitions, List<Bucket> bucketList,
        Map<Integer, Pair<Integer, Integer>> bucketDiff) {
        // Use feign client to pull Kafka data
        List<RowDataHash> dataList = new LinkedList<>();
        poolTopicPartitionsData(getTopicName(endpoint), partitions, dataList);
        rowCount = rowCount + dataList.size();
        if (CollectionUtils.isEmpty(dataList)) {
            return;
        }
        log.debug("initialize the verification data, and pull the total number of [{}-{}-{}] data records to {}",
            endpoint.getDescription(), tableName, partitions, dataList.size());
        BuilderBucketHandler bucketBuilder = new BuilderBucketHandler(bucketCapacity);

        Map<Integer, Bucket> bucketMap = new ConcurrentHashMap<>(InitialCapacity.CAPACITY_128);
        // Use the pulled data to build the bucket list
        bucketBuilder.builder(dataList, tablePartitionRowCount, bucketMap);
        dataList.clear();
        // Statistics bucket list information
        bucketList.addAll(bucketMap.values());
        bucketNoStatistics(endpoint, bucketMap.keySet(), bucketDiff);
        bucketMap.clear();
    }

    private void poolTopicPartitionsData(String topicName, int partitions, List<RowDataHash> dataList) {
        KafkaConsumerHandler consumer = new KafkaConsumerHandler(kafkaConsumerService.buildKafkaConsumer(false),
            kafkaConsumerService.getRetryFetchRecordTimes());
        logKafka.info("create consumer of topic, [{},{}] : [{} : {}] ", tableName, partitions, sourceTopic, sinkTopic);
        consumer.poolTopicPartitionsData(topicName, partitions, dataList);
        consumer.closeConsumer();
        logKafka.info("close consumer of topic, [{},{}] : [{} : {}] ", tableName, partitions, sourceTopic, sinkTopic);
    }

    /**
     * Compare the two Merkel trees and return the difference record.
     *
     * @param sourceTree Source Merkel tree
     * @param sinkTree   Sink Merkel tree
     */
    private void compareMerkleTree(@NonNull MerkleTree sourceTree, @NonNull MerkleTree sinkTree) {
        Node source = sourceTree.getRoot();
        Node sink = sinkTree.getRoot();
        List<Pair<Node, Node>> diffNodeList = new LinkedList<>();
        compareMerkleTree(source, sink, diffNodeList);
        if (CollectionUtils.isEmpty(diffNodeList)) {
            return;
        }
        diffNodeList.forEach(diffNode -> {
            Bucket sourceBucket = diffNode.getSource().getBucket();
            Bucket sinkBucket = diffNode.getSink().getBucket();
            DifferencePair<Map, Map, Map> subDifference = compareBucket(sourceBucket, sinkBucket);
            difference.getDiffering().putAll(subDifference.getDiffering());
            difference.getOnlyOnLeft().putAll(subDifference.getOnlyOnLeft());
            difference.getOnlyOnRight().putAll(subDifference.getOnlyOnRight());
        });
        log.debug("Complete the data verification of table [{}-{}]", tableName, partitions);
        diffNodeList.clear();
    }

    /**
     * Comparison under Merkel tree constraints
     *
     * @param sourceTuple source bucket tuple
     * @param sinkTuple   sink bucket tuple
     */
    private void compareNoMerkleTree(CheckTuple sourceTuple, CheckTuple sinkTuple) {
        // Comparison without Merkel tree constraint
        if (sourceTuple.getBucketSize() == sinkTuple.getBucketSize()) {
            // sourceSize == 0, that is, all buckets are empty
            if (sourceTuple.getBucketSize() == 0) {
                // Table is empty, verification succeeded!
                log.debug("table[{}-{}] is an empty table,this check successful!", tableName, partitions);
            } else {
                // sourceSize is less than thresholdMinBucketSize, that is, there is only one bucket. Compare
                DifferencePair<Map, Map, Map> subDifference =
                    compareBucket(sourceTuple.getBuckets().get(0), sinkTuple.getBuckets().get(0));
                difference.getDiffering().putAll(subDifference.getDiffering());
                difference.getOnlyOnLeft().putAll(subDifference.getOnlyOnLeft());
                difference.getOnlyOnRight().putAll(subDifference.getOnlyOnRight());
            }
            refreshCheckStatus();
        } else {
            refreshCheckStatus();
            throw new LargeDataDiffException(String.format(
                "table[%s] source & sink data have large different," + "source-bucket-count=[%s] sink-bucket-count=[%s]"
                    + " Please synchronize data again! ", tableName, sourceTuple.getBucketSize(),
                sinkTuple.getBucketSize()));
        }
    }

    /**
     * Compare the difference data recorded inside the two barrels
     * <p>
     *
     * @param sourceBucket Source barrel
     * @param sinkBucket   Sink barrel
     * @return Difference record
     */
    private DifferencePair<Map, Map, Map> compareBucket(Bucket sourceBucket, Bucket sinkBucket) {
        Map<String, RowDataHash> sourceMap = sourceBucket.getBucket();
        Map<String, RowDataHash> sinkMap = sinkBucket.getBucket();
        MapDifference<String, RowDataHash> bucketDifference = Maps.difference(sourceMap, sinkMap);
        Map<String, RowDataHash> entriesOnlyOnLeft = bucketDifference.entriesOnlyOnLeft();
        Map<String, RowDataHash> entriesOnlyOnRight = bucketDifference.entriesOnlyOnRight();
        Map<String, MapDifference.ValueDifference<RowDataHash>> entriesDiffering = bucketDifference.entriesDiffering();
        Map<String, Pair<RowDataHash, RowDataHash>> differing = new HashMap<>(Constants.InitialCapacity.EMPTY);
        entriesDiffering.forEach((key, diff) -> {
            differing.put(key, Pair.of(diff.leftValue(), diff.rightValue()));
        });
        return DifferencePair.of(entriesOnlyOnLeft, entriesOnlyOnRight, differing);
    }

    /**
     * <pre>
     * Recursively compare two Merkel tree nodes and record the difference nodes.
     * The recursive preorder traversal method is adopted to traverse and compare the Merkel tree,
     * so as to find the difference node.
     * If the current traversal node {@link org.opengauss.datachecker.check.modules.merkle.MerkleTree.Node}
     * has the same signature, the current traversal branch will be terminated.
     * </pre>
     *
     * @param source       Source Merkel tree node
     * @param sink         Sink Merkel tree node
     * @param diffNodeList Difference node record
     */
    private void compareMerkleTree(Node source, Node sink, List<Pair<Node, Node>> diffNodeList) {
        // If the nodes are the same, exit
        if (Objects.isNull(source) || Objects.isNull(sink)) {
            return;
        }
        if (Arrays.equals(source.getSignature(), sink.getSignature())) {
            return;
        }
        // If the nodes are different, continue to compare the lower level nodes.
        // If the current difference node is a leaf node, record the difference node and exit
        if (source.getType() == MerkleTree.LEAF_SIG_TYPE) {
            diffNodeList.add(Pair.of(source, sink));
            return;
        }
        compareMerkleTree(source.getLeft(), sink.getLeft(), diffNodeList);
        compareMerkleTree(source.getRight(), sink.getRight(), diffNodeList);
    }

    /**
     * <pre>
     * Count the bucket numbers built at each endpoint.
     * The statistical results are summarized in {@code bucketNumberDiffMap}.
     * Merkel 's comparison algorithm needs to ensure that the bucket numbers of both sides are consistent.
     * If the bucket number of one party is missing, that is, in {@code Pair<s, t >}, the value of S or T is -1,
     * you need to generate an empty bucket with the corresponding number.
     * </pre>
     *
     * @param endpoint    end point
     * @param bucketNoSet bucket numbers
     */
    private synchronized void bucketNoStatistics(@NonNull Endpoint endpoint, @NonNull Set<Integer> bucketNoSet,
        Map<Integer, Pair<Integer, Integer>> bucketDiff) {
        bucketNoSet.forEach(bucketNo -> {
            if (!bucketDiff.containsKey(bucketNo)) {
                if (endpoint == Endpoint.SOURCE) {
                    bucketDiff.put(bucketNo, Pair.of(bucketNo, -1));
                } else {
                    bucketDiff.put(bucketNo, Pair.of(-1, bucketNo));
                }
            } else {
                Pair<Integer, Integer> pair = bucketDiff.get(bucketNo);
                if (endpoint == Endpoint.SOURCE) {
                    bucketDiff.put(bucketNo, Pair.of(bucketNo, pair));
                } else {
                    bucketDiff.put(bucketNo, Pair.of(pair, bucketNo));
                }
            }
        });
    }

    private boolean shouldCheckMerkleTree(int sourceBucketCount, int sinkBucketCount) {
        return sourceBucketCount >= THRESHOLD_MIN_BUCKET_SIZE && sinkBucketCount >= THRESHOLD_MIN_BUCKET_SIZE;
    }

    private void checkResult() {
        final AbstractCheckDiffResultBuilder<?, ?> builder = AbstractCheckDiffResultBuilder.builder();
        CheckDiffResult result =
            builder.process(checkParam.getProcess()).table(tableName).topic(sourceTopic).schema(sinkSchema)
                   .conditionLimit(getConditionLimit()).partitions(partitions).isTableStructureEquals(true)
                   .startTime(startTime).endTime(LocalDateTime.now()).isExistTableMiss(false, null).rowCount(rowCount)
                   .errorRate(20).checkMode(CheckMode.FULL).keyUpdateSet(difference.getDiffering().keySet())
                   .keyInsertSet(difference.getOnlyOnLeft().keySet()).keyDeleteSet(difference.getOnlyOnRight().keySet())
                   .build();
        checkResultManagerService.addResult(checkPartition, result);
    }

    private ConditionLimit getConditionLimit() {
        return checkParam.getSourceMetadata().getConditionLimit();
    }

    private void resetThreadName(String tableName, int partitions) {
        Thread.currentThread().setName(tableName + "_p" + partitions);
    }

    private void refreshCheckStatus() {
        tableStatusRegister.update(tableName, partitions, TableStatusRegister.TASK_STATUS_CHECK_VALUE);
    }

    private static String getStatisticsName(String tableName, int partitions) {
        return tableName.concat("_").concat(String.valueOf(partitions));
    }

    private CheckTable buildCheckTable() {
        TableMetadata tableMetadata = endpointMetaDataManager.getTableMetadata(Endpoint.SINK, tableName);
        return CheckTable.builder().tableName(tableName).partition(partitions).rowCount(rowCount).topicName(sinkTopic)
                         .completeTimestamp(System.currentTimeMillis()).avgRowLength(tableMetadata.getAvgRowLength())
                         .build();
    }

    private String getTopicName(Endpoint endpoint) {
        return Objects.equals(Endpoint.SOURCE, endpoint) ? sourceTopic : sinkTopic;
    }

    private void cleanCheckThreadEnvironment() {
        difference.getOnlyOnLeft().clear();
        difference.getOnlyOnRight().clear();
        difference.getDiffering().clear();
    }
}
