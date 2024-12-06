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
import org.opengauss.datachecker.check.modules.check.CheckResultConstants;
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
import org.opengauss.datachecker.common.exception.CheckConsumerPollEmptyException;
import org.opengauss.datachecker.common.exception.MerkleTreeDepthException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.common.util.TopicUtil;
import org.springframework.lang.NonNull;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * SliceCheckWorker
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/2
 * @since ：11
 */
public class SliceCheckWorker implements Runnable {
    private static final Logger LOGGER = LogUtils.getLogger(SliceCheckWorker.class);
    private static final int THRESHOLD_MIN_BUCKET_SIZE = 2;

    private final SliceVo slice;

    private final String processNo;
    private final SliceCheckEvent checkEvent;
    private final SliceCheckContext checkContext;
    private final TaskRegisterCenter registerCenter;
    private final DifferencePair<List<Difference>, List<Difference>, List<Difference>> difference = DifferencePair.of(
        new LinkedList<>(), new LinkedList<>(), new LinkedList<>());
    private final LocalDateTime startTime;

    private long sliceRowCount;
    // 设置最大尝试次数
    private int maxAttemptsTimes;
    private Topic topic = new Topic();

    /**
     * slice check worker construct
     *
     * @param checkEvent check event
     * @param sliceCheckContext slice check context
     */
    public SliceCheckWorker(SliceCheckEvent checkEvent, SliceCheckContext sliceCheckContext,
        TaskRegisterCenter registerCenter) {
        this.checkEvent = checkEvent;
        this.checkContext = sliceCheckContext;
        this.startTime = LocalDateTime.now();
        this.slice = checkEvent.getSlice();
        this.registerCenter = registerCenter;
        this.processNo = ConfigCache.getValue(ConfigConstants.PROCESS_NO);
        this.maxAttemptsTimes = sliceCheckContext.getRetryFetchRecordTimes();
    }

    @Override
    public void run() {
        String errorMsg = "";
        try {
            LogUtils.debug(LOGGER, "check slice of {}", slice.getName());
            SliceExtend source = checkEvent.getSource();
            SliceExtend sink = checkEvent.getSink();
            this.sliceRowCount = Math.max(source.getCount(), sink.getCount());
            setTableFixedTopic();
            SliceTuple sourceTuple = SliceTuple.of(Endpoint.SOURCE, source, new LinkedList<>());
            SliceTuple sinkTuple = SliceTuple.of(Endpoint.SINK, sink, new LinkedList<>());
            // Initialize bucket list
            initBucketList(sourceTuple, sinkTuple);
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
        } catch (Exception ignore) {
            LogUtils.error(LOGGER, "check table has some error,", ignore);
            errorMsg = ignore.getMessage();
        } finally {
            try {
                refreshSliceCheckProgress();
                checkResult(errorMsg);
                cleanCheckThreadEnvironment();
                finishedSliceCheck(slice);
            } catch (Exception exception) {
                LogUtils.error(LOGGER, "refresh check {} error:", slice.getName(), exception);
            }
            LogUtils.info(LOGGER, "check slice of {} end.", slice.getName());
        }
    }

    public void finishedSliceCheck(SliceVo slice) {
        checkContext.saveProcessHistoryLogging(slice);
        registerCenter.refreshAndCheckTableCompleted(slice);
    }

    private void cleanCheckThreadEnvironment() {
        difference.clear();
    }

    private void compareNoMerkleTree(SliceTuple sourceTuple, SliceTuple sinkTuple) {
        // Comparison without Merkel tree constraint
        if (sourceTuple.getBucketSize() == sinkTuple.getBucketSize()) {
            // sourceSize == 0, that is, all buckets are empty
            if (sourceTuple.getBucketSize() == 0) {
                // Table is empty, verification succeeded!
                LogUtils.debug(LOGGER, "slice {} fetch empty", slice.getName());
            } else {
                // sourceSize is less than thresholdMinBucketSize, that is, there is only one bucket. Compare
                DifferencePair<List<Difference>, List<Difference>, List<Difference>> subDifference = null;
                subDifference = compareBucketCommon(sourceTuple.getBuckets().get(0), sinkTuple.getBuckets().get(0));
                difference.getDiffering().addAll(subDifference.getDiffering());
                difference.getOnlyOnLeft().addAll(subDifference.getOnlyOnLeft());
                difference.getOnlyOnRight().addAll(subDifference.getOnlyOnRight());
            }
        } else {
            throw new BucketNumberInconsistentException(String.format(
                "table[%s] slice[%s] build the bucket number is inconsistent, source-bucket-count=[%s] "
                    + "sink-bucket-count=[%s] Please synchronize data again! ", slice.getTable(), slice.getNo(),
                sourceTuple.getBucketSize(), sinkTuple.getBucketSize()));
        }
    }

    private boolean shouldCheckMerkleTree(int sourceBucketCount, int sinkBucketCount) {
        return sourceBucketCount >= THRESHOLD_MIN_BUCKET_SIZE && sinkBucketCount >= THRESHOLD_MIN_BUCKET_SIZE;
    }

    private void checkResult(String resultMsg) {
        CheckDiffResultBuilder builder = CheckDiffResultBuilder.builder();
        int updateTotal = Objects.nonNull(difference.getDiffering()) ? difference.getDiffering().size() : 0;
        int insertTotal = Objects.nonNull(difference.getOnlyOnLeft()) ? difference.getOnlyOnLeft().size() : 0;
        int deleteTotal = Objects.nonNull(difference.getOnlyOnRight()) ? difference.getOnlyOnRight().size() : 0;
        builder.process(ConfigCache.getValue(ConfigConstants.PROCESS_NO))
            .table(slice.getTable())
            .sno(slice.getNo())
            .error(resultMsg)
            .topic(getConcatTableTopics())
            .schema(slice.getSchema())
            .fileName(slice.getName())
            .conditionLimit(getConditionLimit())
            .partitions(slice.getPtnNum())
            .isTableStructureEquals(true)
            .startTime(startTime)
            .endTime(LocalDateTime.now())
            .isExistTableMiss(false, null)
            .rowCount((int) sliceRowCount)
            .updateTotal(updateTotal)
            .insertTotal(insertTotal)
            .deleteTotal(deleteTotal)
            .checkMode(ConfigCache.getValue(ConfigConstants.CHECK_MODE, CheckMode.class))
            .keyDiff(limit(difference.getOnlyOnLeft()), limit(difference.getDiffering()),
                limit(difference.getOnlyOnRight()));
        CheckDiffResult result = builder.build();
        LogUtils.debug(LOGGER, "result {}", result);
        checkContext.addCheckResult(slice, result);
    }

    private List<Difference> limit(List<Difference> differences) {
        return Objects.nonNull(differences)
            ? differences.stream().limit(CheckResultConstants.MAX_DISPLAY_SIZE).collect(Collectors.toList())
            : Collections.emptyList();
    }

    private String getConcatTableTopics() {
        return topic.toTopicString();
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
            DifferencePair<List<Difference>, List<Difference>, List<Difference>> subDifference = compareBucketCommon(
                sourceBucket, sinkBucket);
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
        checkContext.refreshSliceCheckProgress(slice, sliceRowCount);
    }

    private void initBucketList(SliceTuple sourceTuple, SliceTuple sinkTuple) throws InterruptedException {
        List<SliceTuple> checkTupleList = List.of(sourceTuple, sinkTuple);
        Map<Integer, Pair<Integer, Integer>> bucketDiff = new ConcurrentHashMap<>();
        // Get the Kafka partition number corresponding to the current task
        // Initialize source bucket column list data
        long startFetch = System.currentTimeMillis();
        CountDownLatch countDownLatch = new CountDownLatch(checkTupleList.size());
        int avgSliceCount = (int) (sourceTuple.getSlice().getCount() + sinkTuple.getSlice().getCount()) / 2;
        KafkaConsumerHandler consumer = checkContext.createKafkaHandler();
        checkTupleList.forEach(check -> {
            initBucketList(check.getEndpoint(), check.getSlice(), check.getBuckets(), bucketDiff, avgSliceCount,
                consumer);
            countDownLatch.countDown();
        });
        countDownLatch.await();
        checkContext.returnConsumer(consumer);
        LogUtils.debug(LOGGER, "fetch slice {} data from topic, cost {} millis", slice.toSimpleString(),
            costMillis(startFetch));
        // Align the source and destination bucket list
        alignAllBuckets(sourceTuple, sinkTuple, bucketDiff);
        sortBuckets(sourceTuple.getBuckets());
        sortBuckets(sinkTuple.getBuckets());
    }

    private long costMillis(long startMillis) {
        return System.currentTimeMillis() - startMillis;
    }

    private void initBucketList(Endpoint endpoint, SliceExtend sliceExtend, List<Bucket> bucketList,
        Map<Integer, Pair<Integer, Integer>> bucketDiff, int avgSliceCount, KafkaConsumerHandler consumer) {
        // Use feign client to pull Kafka data
        List<RowDataHash> dataList = new LinkedList<>();
        TopicPartition topicPartition = new TopicPartition(
            Objects.equals(Endpoint.SOURCE, endpoint) ? topic.getSourceTopicName() : topic.getSinkTopicName(),
            topic.getPtnNum());
        int attempts = 0;
        while (attempts < maxAttemptsTimes) {
            try {
                consumer.consumerAssign(topicPartition, sliceExtend, attempts);
                consumer.pollTpSliceData(sliceExtend, dataList);
                break; // 如果成功，跳出循环
            } catch (CheckConsumerPollEmptyException ex) {
                if (++attempts >= maxAttemptsTimes) {
                    checkContext.returnConsumer(consumer);
                    throw ex; // 如果达到最大尝试次数，重新抛出异常
                }
                ThreadUtil.sleepOneSecond();
                LogUtils.warn(LOGGER, "poll slice data {} {} , retry ({})", sliceExtend.getName(), sliceExtend.getNo(),
                    attempts);
            }
        }
        if (CollectionUtils.isEmpty(dataList)) {
            return;
        }
        BuilderBucketHandler bucketBuilder = new BuilderBucketHandler(
            ConfigCache.getIntValue(ConfigConstants.BUCKET_CAPACITY));
        Map<Integer, Bucket> bucketMap = new ConcurrentHashMap<>(InitialCapacity.CAPACITY_128);
        // Use the pulled data to build the bucket list
        bucketBuilder.builder(dataList, avgSliceCount, bucketMap);
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

    private void setTableFixedTopic() {
        int maxTopicSize = ConfigCache.getIntValue(ConfigConstants.MAXIMUM_TOPIC_SIZE);
        String table = slice.getTable();
        String sourceTopicName = TopicUtil.getMoreFixedTopicName(processNo, Endpoint.SOURCE, table, maxTopicSize);
        String sinkTopicName = TopicUtil.getMoreFixedTopicName(processNo, Endpoint.SINK, table, maxTopicSize);
        topic.setSourceTopicName(sourceTopicName);
        topic.setSinkTopicName(sinkTopicName);
        topic.setPtnNum(0);
        topic.setPartitions(1);
    }
}
