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
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.cache.CheckRateCache;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.modules.bucket.Bucket;
import org.opengauss.datachecker.check.modules.bucket.BuilderBucketHandler;
import org.opengauss.datachecker.check.modules.merkle.MerkleTree;
import org.opengauss.datachecker.check.modules.merkle.MerkleTree.Node;
import org.opengauss.datachecker.check.modules.report.CheckResultManagerService;
import org.opengauss.datachecker.check.service.EndpointMetaDataManager;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.opengauss.datachecker.common.entry.check.CheckPartition;
import org.opengauss.datachecker.common.entry.check.CheckTable;
import org.opengauss.datachecker.common.entry.check.DifferencePair;
import org.opengauss.datachecker.common.entry.check.IncrementDataCheckParam;
import org.opengauss.datachecker.common.entry.check.Pair;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.TableMetadataHash;
import org.opengauss.datachecker.common.exception.DispatchClientException;
import org.opengauss.datachecker.common.exception.MerkleTreeDepthException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.SpringUtil;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.lang.NonNull;
import org.springframework.util.StopWatch;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * IncrementCheckThread
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class IncrementCheckThread extends Thread {
    private static final Logger log = LogUtils.getLogger();
    private static final int THRESHOLD_MIN_BUCKET_SIZE = 2;
    private static final String THREAD_NAME_PRIFEX = "increment-data-check-";

    private final String tableName;
    private final int bucketCapacity;
    private final int rowCount;
    private final FeignClientService feignClient;
    private final List<Bucket> sourceBucketList = new ArrayList<>();
    private final List<Bucket> sinkBucketList = new ArrayList<>();
    private final DifferencePair<Map<String, RowDataHash>, Map<String, RowDataHash>, Map<String, Pair<Node, Node>>>
            difference = DifferencePair.of(new HashMap<>(), new HashMap<>(), new HashMap<>());
    private final Map<Integer, Pair<Integer, Integer>> bucketNumberDiffMap = new HashMap<>();
    private final QueryRowDataWapper queryRowDataWapper;
    private final CheckResultManagerService checkResultManagerService;
    private final SourceDataLog dataLog;
    private final String process;
    private String sinkSchema;
    private LocalDateTime startTime;
    private boolean isTableStructureEquals;
    private boolean isExistTableMiss;
    private Endpoint onlyExistEndpoint;
    private CheckRateCache checkRateCache;
    private EndpointMetaDataManager endpointMetaDataManager;
    private int maxRowSize;
    private StopWatch stopWatch;

    /**
     * IncrementCheckThread constructor method
     *
     * @param checkParam Data Check Param
     * @param support    Data Check Runnable Support
     */
    public IncrementCheckThread(@NonNull IncrementDataCheckParam checkParam,
                                @NonNull DataCheckRunnableSupport support) {
        startTime = LocalDateTime.now();
        dataLog = checkParam.getDataLog();
        process = checkParam.getProcess();
        sinkSchema = checkParam.getSchema();
        rowCount = dataLog.getCompositePrimaryValues().size();
        tableName = checkParam.getTableName();
        bucketCapacity = checkParam.getBucketCapacity();
        feignClient = support.getFeignClientService();
        checkResultManagerService = support.getCheckResultManagerService();
        checkRateCache = SpringUtil.getBean(CheckRateCache.class);
        endpointMetaDataManager = SpringUtil.getBean(EndpointMetaDataManager.class);
        queryRowDataWapper = new QueryRowDataWapper(feignClient);
        stopWatch = new StopWatch("inc " + sinkSchema + "." + tableName);
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
            stopWatch.start("checkTableMetadata");
            setName(buildThreadName());
            // Metadata verification
            isTableStructureEquals = checkTableMetadata();
            stopWatch.stop();
            if (isTableStructureEquals) {
                maxRowSize = dataLog.getCompositePrimaryValues().size();
                stopWatch.start("firstCheckCompare " + maxRowSize);
                // Initial verification
                firstCheckCompare();
                stopWatch.stop();
                // Analyze the initial verification results
                stopWatch.start("secondaryCheckCompare");
                List<String> diffIdList = parseDiffResult();
                // Conduct secondary verification according to the initial verification results
                secondaryCheckCompare(diffIdList);
                stopWatch.stop();
            } else {
                log.error("check table {} metadata error", tableName);
            }
            // Verification result verification repair report
            stopWatch.start("checkResult");
            checkResult();
            checkRateCache.add(buildCheckTable());
            stopWatch.stop();
        } catch (Exception ex) {
            log.error("check error", ex);
        } finally {
            log.info(" {} check {} ", process, stopWatch.shortSummary());
        }
    }

    private CheckTable buildCheckTable() {
        TableMetadata tableMetadata = endpointMetaDataManager.queryIncrementMetaData(Endpoint.SINK, tableName);
        return CheckTable.builder().tableName(tableName).rowCount(rowCount)
                .completeTimestamp(System.currentTimeMillis()).avgRowLength(tableMetadata.getAvgRowLength())
                .build();
    }

    /**
     * Initial verification
     */
    private void firstCheckCompare() {
        // Initialize bucket list
        initFirstCheckBucketList();
        compareCommonMerkleTree();
    }

    /**
     * the second check
     *
     * @param diffIdList Initial verification difference ID list
     */
    private void secondaryCheckCompare(List<String> diffIdList) {
        if (CollectionUtils.isEmpty(diffIdList)) {
            return;
        }
        // Clean up the current thread pinch check cache information
        lastDataClean();
        // Initialize bucket list
        initSecondaryCheckBucketList(diffIdList);
        // Conduct secondary verification
        compareCommonMerkleTree();
    }

    /**
     * Initialize bucket list
     */
    private void initFirstCheckBucketList() {
        // Get the Kafka partition number corresponding to the current task
        // Initialize source bucket column list data
        initFirstCheckBucketList(Endpoint.SOURCE, sourceBucketList);
        // Initialize destination bucket column list data
        initFirstCheckBucketList(Endpoint.SINK, sinkBucketList);
        // Align the source and destination bucket list
        alignAllBuckets();
        sortBuckets(sourceBucketList);
        sortBuckets(sinkBucketList);
    }

    private void initSecondaryCheckBucketList(List<String> diffIdList) {
        dataLog.setCompositePrimaryValues(diffIdList);
        buildSecondaryCheckBucket(Endpoint.SOURCE, dataLog, sourceBucketList);
        buildSecondaryCheckBucket(Endpoint.SINK, dataLog, sinkBucketList);
        // Align the source and destination bucket list
        alignAllBuckets();
        sortBuckets(sourceBucketList);
        sortBuckets(sinkBucketList);
    }

    private void compareCommonMerkleTree() {
        // No Merkel tree verification algorithm scenario
        if (!shouldCheckMerkleTree(sourceBucketList.size(), sinkBucketList.size())) {
            compareNoMerkleTree(sourceBucketList.size(), sinkBucketList.size());
        } else {
            // Construct Merkel tree constraint: bucketList cannot be empty, and size > =2
            MerkleTree sourceTree = new MerkleTree(sourceBucketList);
            MerkleTree sinkTree = new MerkleTree(sinkBucketList);

            // Recursively compare two Merkel trees and return the difference record.
            compareMerkleTree(sourceTree, sinkTree);
        }
    }

    private void compareNoMerkleTree(int sourceBucketCount, int sinkBucketCount) {
        // Comparison without Merkel tree constraint
        if (sourceBucketCount == sinkBucketCount) {
            // sourceSize == 0, that is, all buckets are empty
            if (sourceBucketCount == 0) {
                // Table is empty, verification succeeded!
                log.info("table[{}.{}] is an empty table,this check successful!", sinkSchema, tableName);
            } else {
                // sourceSize is less than thresholdMinBucketSize, that is, there is only one bucket. Compare
                DifferencePair<Map, Map, Map> subDifference =
                        compareBucket(sourceBucketList.get(0), sinkBucketList.get(0));
                difference.getDiffering().putAll(subDifference.getDiffering());
                difference.getOnlyOnLeft().putAll(subDifference.getOnlyOnLeft());
                difference.getOnlyOnRight().putAll(subDifference.getOnlyOnRight());
            }
        }
    }

    private void lastDataClean() {
        sourceBucketList.clear();
        sinkBucketList.clear();
        bucketNumberDiffMap.clear();
        difference.getOnlyOnRight().clear();
        difference.getOnlyOnLeft().clear();
        difference.getDiffering().clear();
    }

    /**
     * Sort the final bucket list by bucket number
     *
     * @param bucketList bucketList
     */
    private void sortBuckets(@NonNull List<Bucket> bucketList) {
        bucketList.sort(Comparator.comparingInt(Bucket::getNumber));
    }

    private List<String> parseDiffResult() {
        List<String> diffKeyList = new ArrayList<>();
        diffKeyList.addAll(difference.getDiffering().keySet());
        diffKeyList.addAll(difference.getOnlyOnRight().keySet());
        diffKeyList.addAll(difference.getOnlyOnLeft().keySet());
        return diffKeyList;
    }

    /**
     * <pre>
     * The precondition of incremental verification is that the current table structure is consistent.
     * If the table structure is inconsistent, exit directly. No data verification
     * </pre>
     *
     * @return Return metadata verification results
     */
    private boolean checkTableMetadata() {
        TableMetadataHash sourceTableHash = querySourceTableMetadataHash(tableName);
        TableMetadataHash sinkTableHash = querySinkTableMetadataHash(tableName);
        boolean isEqual = Objects.equals(sourceTableHash, sinkTableHash);
        if (!isEqual) {
            isExistTableMiss = true;
            if (sourceTableHash.getTableHash() == -1) {
                onlyExistEndpoint = Endpoint.SINK;
            } else if (sinkTableHash.getTableHash() == -1) {
                onlyExistEndpoint = Endpoint.SOURCE;
            } else {
                onlyExistEndpoint = null;
            }
        } else {
            isExistTableMiss = false;
        }
        return isEqual;
    }

    private TableMetadataHash querySourceTableMetadataHash(String tableName) {
        Result<TableMetadataHash> result = feignClient.getClient(Endpoint.SOURCE)
                .querySourceTableMetadataHash(tableName);
        if (result.isSuccess()) {
            return result.getData();
        } else {
            throw new DispatchClientException(
                    Endpoint.SOURCE,
                    "query table metadata hash " + tableName + " error, " + result.getMessage());
        }
    }

    private TableMetadataHash querySinkTableMetadataHash(String tableName) {
        Result<TableMetadataHash> result = feignClient.getClient(Endpoint.SINK)
                .querySinkTableMetadataHash(tableName);
        if (result.isSuccess()) {
            return result.getData();
        } else {
            throw new DispatchClientException(Endpoint.SINK,
                    "query table metadata hash " + tableName + " error, " + result.getMessage());
        }
    }

    /**
     * Comparison without Merkel tree constraint
     *
     * @param sourceBucketCount source bucket count
     * @param sinkBucketCount   sink bucket count
     * @return Whether it meets the Merkel verification scenario
     */
    private boolean shouldCheckMerkleTree(int sourceBucketCount, int sinkBucketCount) {
        return sourceBucketCount >= THRESHOLD_MIN_BUCKET_SIZE && sinkBucketCount >= THRESHOLD_MIN_BUCKET_SIZE;
    }

    /**
     * Compare the two Merkel trees and return the difference record.
     *
     * @param sourceTree source tree
     * @param sinkTree   sink tree
     */
    private void compareMerkleTree(@NonNull MerkleTree sourceTree, @NonNull MerkleTree sinkTree) {
        // Merkel tree comparison
        if (sourceTree.getDepth() != sinkTree.getDepth()) {
            throw new MerkleTreeDepthException(String.format(Locale.ROOT,
                    "source & sink data have large different, Please synchronize data again! "
                            + "merkel tree depth different,source depth=[%d],sink depth=[%d]", sourceTree.getDepth(),
                    sinkTree.getDepth()));
        }
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
    }

    /**
     * Align the bucket list data according to the statistical results of source and destination bucket
     * difference information {@code bucketNumberDiffMap}.
     */
    private void alignAllBuckets() {
        new DataCheckWapper().alignAllBuckets(bucketNumberDiffMap, sourceBucketList, sinkBucketList);
    }

    /**
     * <pre>
     * Pull the Kafka partition {@code partitions} data of the current table {@code tableName} of
     * the specified endpoint {@code endpoint} service.
     * And assemble Kafka data into the specified bucket list {@code bucketList}
     * </pre>
     *
     * @param endpoint   endpoint
     * @param bucketList bucket list
     */
    private void initFirstCheckBucketList(Endpoint endpoint, List<Bucket> bucketList) {
        StopWatch rowDataQueryWatch = new StopWatch("first query row data " + endpoint);
        rowDataQueryWatch.start(dataLog.getTableName() + " " + dataLog.getCompositePrimaryValues().size());
        List<RowDataHash> dataList = queryRowDataWapper.queryCheckRowData(endpoint, dataLog);
        rowDataQueryWatch.stop();
        LogUtils.debug(log, "query row data cost: {}", rowDataQueryWatch.shortSummary());
        buildBucket(dataList, endpoint, bucketList);
    }

    private void buildBucket(List<RowDataHash> dataList, Endpoint endpoint, List<Bucket> bucketList) {
        if (CollectionUtils.isEmpty(dataList)) {
            return;
        }
        Map<Integer, Bucket> bucketMap = new HashMap<>(InitialCapacity.CAPACITY_16);
        BuilderBucketHandler bucketBuilder = new BuilderBucketHandler(bucketCapacity);

        // Pull the data to build the bucket list
        bucketBuilder.builder(dataList, maxRowSize, bucketMap);
        // Statistics bucket list information
        bucketNumberStatisticsIncrement(endpoint, bucketMap.keySet());
        bucketList.addAll(bucketMap.values());
    }

    private void buildSecondaryCheckBucket(Endpoint endpoint, SourceDataLog dataLog, List<Bucket> bucketList) {
        StopWatch rowDataSecQueryWatch = new StopWatch("check sec query row data " + endpoint);
        rowDataSecQueryWatch.start(dataLog.getTableName() + " " + dataLog.getCompositePrimaryValues().size());
        List<RowDataHash> dataList = queryRowDataWapper.querySecondaryCheckRowData(endpoint, dataLog);
        rowDataSecQueryWatch.stop();
        LogUtils.debug(log, "query sec row data cost: {}", rowDataSecQueryWatch.shortSummary());
        buildBucket(dataList, endpoint, bucketList);
    }

    /**
     * <pre>
     * Count the bucket numbers built at each endpoint.
     * The statistical results are summarized in {@code bucketNumberDiffMap}.
     * Merkel  comparison algorithm needs to ensure that the bucket numbers of both sides are consistent.
     * If the bucket number of one party is missing, that is, in {@code Pair<s, t >}, the value of S or T is -1,
     * you need to generate an empty bucket with the corresponding number.
     * </pre>
     *
     * @param endpoint        endpoint
     * @param bucketNumberSet bucket numbers
     */
    private void bucketNumberStatisticsIncrement(@NonNull Endpoint endpoint, @NonNull Set<Integer> bucketNumberSet) {
        bucketNumberSet.forEach(bucketNumber -> {
            if (!bucketNumberDiffMap.containsKey(bucketNumber)) {
                if (Objects.equals(endpoint, Endpoint.SOURCE)) {
                    bucketNumberDiffMap.put(bucketNumber, Pair.of(bucketNumber, -1));
                } else {
                    bucketNumberDiffMap.put(bucketNumber, Pair.of(-1, bucketNumber));
                }
            } else {
                Pair<Integer, Integer> pair = bucketNumberDiffMap.get(bucketNumber);
                if (Objects.equals(endpoint, Endpoint.SOURCE)) {
                    bucketNumberDiffMap.put(bucketNumber, Pair.of(bucketNumber, pair));
                } else {
                    bucketNumberDiffMap.put(bucketNumber, Pair.of(pair, bucketNumber));
                }
            }
        });
    }

    /**
     * Compare the difference data recorded inside the two barrels
     *
     * @param sourceBucket Source end barrel
     * @param sinkBucket   Sink end barrel
     * @return Difference Pair record
     */
    private DifferencePair<Map, Map, Map> compareBucket(Bucket sourceBucket, Bucket sinkBucket) {
        if (sourceBucket == null || sinkBucket == null) {
            return DifferencePair.of(sourceBucket == null ? sinkBucket.getBucket() : new HashMap<>(),
                    sinkBucket == null ? sourceBucket.getBucket() : new HashMap<>(), new HashMap());
        }
        Map<String, RowDataHash> sourceMap = sourceBucket.getBucket();
        Map<String, RowDataHash> sinkMap = sinkBucket.getBucket();
        MapDifference<String, RowDataHash> bucketDifference = Maps.difference(sourceMap, sinkMap);
        Map<String, RowDataHash> entriesOnlyOnLeft = bucketDifference.entriesOnlyOnLeft();
        Map<String, RowDataHash> entriesOnlyOnRight = bucketDifference.entriesOnlyOnRight();
        Map<String, MapDifference.ValueDifference<RowDataHash>> entriesDiffering = bucketDifference.entriesDiffering();
        Map<String, Pair<RowDataHash, RowDataHash>> differing = new HashMap<>(InitialCapacity.CAPACITY_16);
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
     * If the current traversal node {@link Node} has the same signature,
     * the current traversal branch will be terminated.
     * </pre>
     *
     * @param source       Source Merkel tree node
     * @param sink         Sink Merkel tree node
     * @param diffNodeList Difference node record
     */
    private void compareMerkleTree(Node source, Node sink, List<Pair<Node, Node>> diffNodeList) {
        // If the nodes are the same, exit
        if (source == null || sink == null) {
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

    private void checkResult() {
        final AbstractCheckDiffResultBuilder<?, ?> builder = AbstractCheckDiffResultBuilder.builder();
        CheckDiffResult result =
                builder.table(tableName).process(process).beginOffset(dataLog.getBeginOffset()).schema(sinkSchema)
                        .partitions(0).rowCount(rowCount).startTime(startTime).endTime(LocalDateTime.now())
                        .isExistTableMiss(isExistTableMiss, onlyExistEndpoint).checkMode(CheckMode.INCREMENT)
                        .isTableStructureEquals(isTableStructureEquals)
                        .keyUpdateSet(difference.getDiffering().keySet())
                        .keyInsertSet(difference.getOnlyOnLeft().keySet())
                        .keyDeleteSet(difference.getOnlyOnRight().keySet())
                        .build();
        checkResultManagerService.addResult(new CheckPartition(tableName, 0), result);
    }

    private String buildThreadName() {
        return THREAD_NAME_PRIFEX + tableName;
    }
}
