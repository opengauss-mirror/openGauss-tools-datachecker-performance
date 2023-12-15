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

package org.opengauss.datachecker.check.slice;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.modules.bucket.Bucket;
import org.opengauss.datachecker.check.modules.bucket.BuilderBucketHandler;
import org.opengauss.datachecker.check.modules.bucket.SliceTuple;
import org.opengauss.datachecker.check.modules.check.AbstractCheckDiffResultBuilder.CheckDiffResultBuilder;
import org.opengauss.datachecker.check.modules.check.CheckDiffResult;
import org.opengauss.datachecker.check.modules.check.KafkaConsumerHandler;
import org.opengauss.datachecker.check.modules.merkle.MerkleTree;
import org.opengauss.datachecker.check.modules.merkle.MerkleTree.Node;
import org.opengauss.datachecker.check.service.TaskRegisterCenter;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.opengauss.datachecker.common.entry.check.Difference;
import org.opengauss.datachecker.common.entry.check.DifferencePair;
import org.opengauss.datachecker.common.entry.check.Pair;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ConditionLimit;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.exception.BucketNumberInconsistentException;
import org.opengauss.datachecker.common.exception.MerkleTreeDepthException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.SpringUtil;
import org.springframework.lang.NonNull;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * TableCheckWorker
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/2
 * @since ：11
 */
public class TableCheckWorker implements Runnable {
    private static final Logger log = LogUtils.getBusinessLogger();
    private static final Logger logKafka = LogUtils.getKafkaLogger();
    private static final int THRESHOLD_MIN_BUCKET_SIZE = 2;

    private final SliceVo slice;
    private long sliceRowCont;
    private final SliceCheckEvent checkEvent;
    private final SliceCheckContext checkContext;
    private final DifferencePair<List<Difference>, List<Difference>, List<Difference>> difference =
        DifferencePair.of(new LinkedList<>(), new LinkedList<>(), new LinkedList<>());
    private final LocalDateTime startTime;

    /**
     * slice check worker construct
     *
     * @param checkEvent        check event
     * @param sliceCheckContext slice check context
     */
    public TableCheckWorker(SliceCheckEvent checkEvent, SliceCheckContext sliceCheckContext) {
        this.checkEvent = checkEvent;
        this.checkContext = sliceCheckContext;
        this.startTime = LocalDateTime.now();
        this.slice = checkEvent.getSlice();
    }

    @Override
    public void run() {
        String errorMsg = "";
        try {
            SliceExtend source = checkEvent.getSource();
            SliceExtend sink = checkEvent.getSink();
            this.sliceRowCont = Math.max(source.getCount(), sink.getCount());
            log.info("check table of {}", slice.getName());
            Topic topic = checkContext.getTopic(slice.getTable());
            int ptnNum = topic.getPtnNum();
            for (int ptn = 0; ptn < ptnNum; ptn++) {
                log.info("check table {} of TopicPartition {}:{}", slice.getName(), ptnNum, ptn);
                checkedTableSliceByTopicPartition(source, sink, ptn);
            }
        } catch (Exception ignore) {
            log.error("check table has some error,", ignore);
            errorMsg = ignore.getMessage();
        } finally {
            refreshSliceCheckProgress();
            checkResult(errorMsg);
            cleanCheckThreadEnvironment();
            finishedTableCheck();
            log.info("check table {} end, and will drop table topics .", slice.getName());
        }
    }

    private void finishedTableCheck() {
        TaskRegisterCenter registerCenter = SpringUtil.getBean(TaskRegisterCenter.class);
        if (registerCenter.refreshAndCheckTableCompleted(slice)) {
            dropTableTopics();
        }
    }

    private void dropTableTopics() {
        checkContext.dropTableTopics(slice.getTable());
    }

    private void checkedTableSliceByTopicPartition(SliceExtend source, SliceExtend sink, int ptn)
        throws InterruptedException {
        SliceTuple sourceTuple = SliceTuple.of(Endpoint.SOURCE, source, new LinkedList<>());
        SliceTuple sinkTuple = SliceTuple.of(Endpoint.SINK, sink, new LinkedList<>());
        // Initialize bucket list
        initBucketList(sourceTuple, sinkTuple, ptn);
        // No Merkel tree verification algorithm scenario
        if (shouldCheckMerkleTree(sourceTuple.getBucketSize(), sinkTuple.getBucketSize())) {
            // Construct Merkel tree constraint: bucketList cannot be empty, and size > =2
            MerkleTree sourceTree = new MerkleTree(sourceTuple.getBuckets());
            MerkleTree sinkTree = new MerkleTree(sinkTuple.getBuckets());
            // Merkel tree comparison
            if (sourceTree.getDepth() != sinkTree.getDepth()) {
                throw new MerkleTreeDepthException(String.format(Locale.ROOT,
                    "source & sink data have large different, Please synchronize data again! "
                        + "merkel tree depth different,source depth=[%d],sink depth=[%d]", sourceTree.getDepth(),
                    sinkTree.getDepth()));
            }
            // Recursively compare two Merkel trees and return the difference record.
            compareMerkleTree(sourceTree, sinkTree);
        } else {
            compareNoMerkleTree(sourceTuple, sinkTuple);
        }
    }

    private void cleanCheckThreadEnvironment() {
        difference.getOnlyOnLeft().clear();
        difference.getOnlyOnRight().clear();
        difference.getDiffering().clear();
    }

    private void compareNoMerkleTree(SliceTuple sourceTuple, SliceTuple sinkTuple) {
        // Comparison without Merkel tree constraint
        if (sourceTuple.getBucketSize() == sinkTuple.getBucketSize()) {
            // sourceSize == 0, that is, all buckets are empty
            if (sourceTuple.getBucketSize() == 0) {
                // Table is empty, verification succeeded!
                log.info("slice {} fetch empty", slice.getName());
            } else {
                // sourceSize is less than thresholdMinBucketSize, that is, there is only one bucket. Compare
                DifferencePair<List<Difference>, List<Difference>, List<Difference>> subDifference =
                    compareBucketCommon(sourceTuple.getBuckets().get(0), sinkTuple.getBuckets().get(0));
                difference.getDiffering().addAll(subDifference.getDiffering());
                difference.getOnlyOnLeft().addAll(subDifference.getOnlyOnLeft());
                difference.getOnlyOnRight().addAll(subDifference.getOnlyOnRight());
            }
        } else {
            throw new BucketNumberInconsistentException(String.format(
                "table[%s] slice[%s] build the bucket number is inconsistent, source-bucket-count=[%s] sink-bucket-count=[%s]"
                    + " Please synchronize data again! ", slice.getTable(), slice.getNo(), sourceTuple.getBucketSize(),
                sinkTuple.getBucketSize()));
        }
    }

    private boolean shouldCheckMerkleTree(int sourceBucketCount, int sinkBucketCount) {
        return sourceBucketCount >= THRESHOLD_MIN_BUCKET_SIZE && sinkBucketCount >= THRESHOLD_MIN_BUCKET_SIZE;
    }

    private void checkResult(String resultMsg) {
        CheckDiffResultBuilder builder = CheckDiffResultBuilder.builder();
        builder.process(ConfigCache.getValue(ConfigConstants.PROCESS_NO)).table(slice.getTable()).sno(slice.getNo())
               .error(resultMsg).topic(getConcatTableTopics()).schema(slice.getSchema())
               .conditionLimit(getConditionLimit()).partitions(slice.getPtn()).isTableStructureEquals(true)
               .startTime(startTime).endTime(LocalDateTime.now()).isExistTableMiss(false, null)
               .rowCount((int) sliceRowCont).errorRate(20).fileName(slice.getName())
               .checkMode(ConfigCache.getValue(ConfigConstants.CHECK_MODE, CheckMode.class))
               .keyDiff(difference.getOnlyOnLeft(), difference.getDiffering(), difference.getOnlyOnRight());
        CheckDiffResult result = builder.build();
        checkContext.addCheckResult(slice, result);
    }

    private String getConcatTableTopics() {
        return checkContext.getTopicName(slice.getTable());
    }

    private ConditionLimit getConditionLimit() {
        return null;
    }

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
            DifferencePair<List<Difference>, List<Difference>, List<Difference>> subDifference =
                compareBucketCommon(sourceBucket, sinkBucket);
            difference.getDiffering().addAll(subDifference.getDiffering());
            difference.getOnlyOnLeft().addAll(subDifference.getOnlyOnLeft());
            difference.getOnlyOnRight().addAll(subDifference.getOnlyOnRight());
        });
        diffNodeList.clear();
    }

    private DifferencePair<List<Difference>, List<Difference>, List<Difference>> compareBucketCommon(
        Bucket sourceBucket, Bucket sinkBucket) {
        Map<String, RowDataHash> sourceMap = sourceBucket.getBucket();
        Map<String, RowDataHash> sinkMap = sinkBucket.getBucket();
        MapDifference<String, RowDataHash> bucketDifference = Maps.difference(sourceMap, sinkMap);
        List<Difference> entriesOnlyOnLeft = collectorDeleteOrInsert(bucketDifference.entriesOnlyOnLeft());
        List<Difference> entriesOnlyOnRight = collectorDeleteOrInsert(bucketDifference.entriesOnlyOnRight());
        List<Difference> differing = collectorUpdate(bucketDifference.entriesDiffering());
        return DifferencePair.of(entriesOnlyOnLeft, entriesOnlyOnRight, differing);
    }

    private List<Difference> collectorDeleteOrInsert(Map<String, RowDataHash> diffMaps) {
        List<Difference> result = new LinkedList<>();
        diffMaps.forEach((key, diff) -> {
            result.add(new Difference(key, diff.getIdx()));
        });
        return result;
    }

    private List<Difference> collectorUpdate(Map<String, MapDifference.ValueDifference<RowDataHash>> diffMaps) {
        List<Difference> result = new LinkedList<>();
        diffMaps.forEach((key, diff) -> {
            RowDataHash rowDataHash = diff.leftValue();
            result.add(new Difference(key, rowDataHash.getIdx()));
        });
        return result;
    }

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

    private void refreshSliceCheckProgress() {
        checkContext.refreshSliceCheckProgress(slice, sliceRowCont);
    }

    private void initBucketList(SliceTuple sourceTuple, SliceTuple sinkTuple, int ptn) throws InterruptedException {
        List<SliceTuple> checkTupleList = List.of(sourceTuple, sinkTuple);
        Map<Integer, Pair<Integer, Integer>> bucketDiff = new ConcurrentHashMap<>();
        // Get the Kafka partition number corresponding to the current task
        // Initialize source bucket column list data
        CountDownLatch countDownLatch = new CountDownLatch(checkTupleList.size());
        checkTupleList.parallelStream().forEach(check -> {
            String topicName = checkContext.getTopicName(slice.getTable(), check.getEndpoint());
            TopicPartition topicPartition = new TopicPartition(topicName, ptn);
            initBucketList(check.getEndpoint(), topicPartition, check.getBuckets(), bucketDiff);
            countDownLatch.countDown();
        });
        countDownLatch.await();

        // Align the source and destination bucket list
        alignAllBuckets(sourceTuple, sinkTuple, bucketDiff);
        sortBuckets(sourceTuple.getBuckets());
        sortBuckets(sinkTuple.getBuckets());
    }

    private void initBucketList(Endpoint endpoint, TopicPartition topicPartition, List<Bucket> bucketList,
        Map<Integer, Pair<Integer, Integer>> bucketDiff) {
        // Use feign client to pull Kafka data
        List<RowDataHash> dataList = new LinkedList<>();
        getSliceDataFromTopicPartition(topicPartition, dataList);

        if (CollectionUtils.isEmpty(dataList)) {
            return;
        }
        BuilderBucketHandler bucketBuilder =
            new BuilderBucketHandler(ConfigCache.getIntValue(ConfigConstants.BUCKET_CAPACITY));

        Map<Integer, Bucket> bucketMap = new ConcurrentHashMap<>(InitialCapacity.CAPACITY_128);
        // Use the pulled data to build the bucket list
        bucketBuilder.builder(dataList, dataList.size(), bucketMap);
        dataList.clear();
        // Statistics bucket list information
        bucketList.addAll(bucketMap.values());
        bucketNoStatistics(endpoint, bucketMap.keySet(), bucketDiff);
        bucketMap.clear();
    }

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

    private void sortBuckets(@NonNull List<Bucket> bucketList) {
        bucketList.sort(Comparator.comparingInt(Bucket::getNumber));
    }

    private void getSliceDataFromTopicPartition(TopicPartition topicPartition, List<RowDataHash> dataList) {
        KafkaConsumerHandler consumer = checkContext.buildKafkaHandler();
        logKafka.debug("create consumer of topic, [{}] : [{}] ", slice.getTable(), topicPartition.toString());
        consumer.poolTopicPartitionsData(topicPartition.topic(), topicPartition.partition(), dataList);
        consumer.closeConsumer();
        logKafka.debug("close consumer of topic, [{}] : [{}] ", slice.getTable(), topicPartition.toString());
    }

    /**
     * <pre>
     * Align the bucket list data according to the statistical results of source
     * and destination bucket difference information {@code bucketNumberDiffMap}.
     * </pre>
     */
    private void alignAllBuckets(SliceTuple sourceTuple, SliceTuple sinkTuple,
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
}
