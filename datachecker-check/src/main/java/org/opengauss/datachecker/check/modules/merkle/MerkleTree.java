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

package org.opengauss.datachecker.check.modules.merkle;

import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.modules.bucket.Bucket;
import org.opengauss.datachecker.common.util.ByteUtil;
import org.opengauss.datachecker.common.util.LogUtils;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.zip.Adler32;

/**
 * MerkleTree
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Data
public class MerkleTree {
    private static final Logger log = LogUtils.getBusinessLogger();
    public static final int MAGIC_HDR = 0Xcdaace99;
    public static final int LONG_BYTE = 8;
    /**
     * Merkel tree node type leaf node
     */
    public static final byte LEAF_SIG_TYPE = 0x0;
    /**
     * Merkel tree node type internal node
     */
    public static final byte INTERNAL_SIG_TYPE = 0x01;

    private static final int INT_BYTE = 4;
    /**
     * Serialization format ：(magic header:int)(num nodes:int)(tree depth:int)(leaf length:int)
     * [(node type:byte)(signature length:int)(signature:byte)]
     */
    private static final int MAGIC_HEADER_BYTE_LENGTH = INT_BYTE;
    private static final int LEAF_SIGNATURE_BYTE_LENGTH = INT_BYTE;
    private static final int NUM_NODES_BYTE_LENGTH = INT_BYTE;
    private static final int TREE_DEPTH_BYTE_LENGTH = INT_BYTE;
    private static final int NODE_TYPE_BYTE_LENGTH = 1;

    /**
     * Adler32 for verification
     */
    private volatile static Adler32 crc;

    static {
        crc = new Adler32();
    }

    /**
     * The leaf node byte length Merkel tree is used in serialization and deserialization.
     */
    private int leafSignatureByteLength;
    /**
     * Root node
     */
    private Node root;
    private int depth;
    private int nnodes;

    /**
     * Merkel tree constructor
     *
     * @param bucketList bucketList
     */
    public MerkleTree(List<Bucket> bucketList) {
        constructTree(bucketList);
    }

    /**
     * Construct Merkel tree according to the result of deserialization
     *
     * @param treeRoot   treeRoot
     * @param totalNodes totalNodes
     * @param depth      depth
     * @param leafLength leaf Length
     */
    public MerkleTree(Node treeRoot, int totalNodes, int depth, int leafLength) {
        root = treeRoot;
        nnodes = totalNodes;
        this.depth = depth;
        leafSignatureByteLength = leafLength;
    }

    /**
     * Construct Merkel tree
     *
     * @param bucketList bucketList
     */
    private void constructTree(List<Bucket> bucketList) {
        if (bucketList == null || bucketList.size() < MerkleConstant.CONSTRUCT_TREE_MIN_SIZE) {
            throw new IllegalArgumentException("ERROR:Fail to construct merkle tree ! leafHashes data invalid !");
        }
        nnodes = bucketList.size();
        List<Node> parents = buttomLevel(bucketList);
        nnodes += parents.size();
        depth = 1;
        while (parents.size() > 1) {
            parents = constructInternalLevel(parents);
            depth++;
            nnodes += parents.size();
        }
        root = parents.get(0);
    }

    /**
     * Bottom level leaf node construction
     *
     * @param bucketList bucketList
     * @return node list
     */
    private List<Node> buttomLevel(List<Bucket> bucketList) {
        List<Node> parents = new ArrayList<>(bucketList.size() / MerkleConstant.EVEN_NUMBER);
        for (int i = 0; i < bucketList.size() - 1; i = i + MerkleConstant.EVEN_NUMBER) {
            Node leaf1 = constructLeafNode(bucketList.get(i));
            Node leaf2 = constructLeafNode(bucketList.get(i + 1));

            Node parent = constructInternalNode(leaf1, leaf2);
            parents.add(parent);
        }
        if (bucketList.size() % MerkleConstant.EVEN_NUMBER == 1) {
            Node leaf1 = constructLeafNode(bucketList.get(bucketList.size() - 1));
            // In the case of an odd number of nodes, copy the last node
            Node parent = constructInternalNode(leaf1, null);
            parents.add(parent);
        }
        // Set leaf node signature byte length
        leafSignatureByteLength = parents.get(0).getLeft().getSignature().length;
        return parents;
    }

    /**
     * Internal node construction
     *
     * @param children child node
     * @return Internal node set
     */
    private List<Node> constructInternalLevel(List<Node> children) {
        List<Node> parents = new ArrayList<>(children.size() / MerkleConstant.EVEN_NUMBER);
        for (int i = 0; i < children.size() - 1; i = i + MerkleConstant.EVEN_NUMBER) {
            Node parent = constructInternalNode(children.get(i), children.get(i + 1));
            parents.add(parent);
        }

        if (children.size() % MerkleConstant.EVEN_NUMBER == 1) {
            // In the case of an odd number of nodes, only the left node is calculated
            Node parent = constructInternalNode(children.get(children.size() - 1), null);
            parents.add(parent);
        }
        return parents;
    }

    /**
     * Building leaf nodes
     *
     * @param bucket Bucket node
     * @return Merkel node
     */
    private Node constructLeafNode(Bucket bucket) {
        return new Node().setType(LEAF_SIG_TYPE).setBucket(bucket).setSignature(bucket.getSignature());
    }

    /**
     * Build internal nodes
     *
     * @param left  Left node
     * @param right Right node
     * @return Merkel node
     */
    private Node constructInternalNode(Node left, Node right) {
        return new Node().setType(INTERNAL_SIG_TYPE).setLeft(left).setRight(right)
                         .setSignature(internalSignature(left, right));
    }

    /**
     * Calculate internal node signature
     *
     * @param left  Left node
     * @param right Right node
     * @return Internal node signature
     */
    private byte[] internalSignature(Node left, Node right) {
        if (right == null) {
            return left.getSignature();
        }
        // Here, deler32 is used for signature
        crc.reset();
        crc.update(left.signature);
        crc.update(right.signature);
        return ByteUtil.toBytes(crc.getValue());
    }

    /**
     * Serialization format ：
     * header (magic header:int)(num nodes:int)(tree depth:int)(leaf length:int)
     * [(node type:byte)(signature length:int)(signature:byte)(bucket length:int)(bucket:byte)]
     * <p>
     * bucket Bucket serialization implementation
     *
     * @return Return serialized byte stream
     */
    public byte[] serialize() {
        int header =
            MAGIC_HEADER_BYTE_LENGTH + NUM_NODES_BYTE_LENGTH + TREE_DEPTH_BYTE_LENGTH + LEAF_SIGNATURE_BYTE_LENGTH;
        int maxSignatureByteLength = Math.max(leafSignatureByteLength, LONG_BYTE);

        int spaceOfNodes = (NODE_TYPE_BYTE_LENGTH + NUM_NODES_BYTE_LENGTH + maxSignatureByteLength) * nnodes;

        int capacity = header + spaceOfNodes;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        //header (magic header:int)(num nodes:int)(tree depth:int)(leaf length:int)
        buffer.putInt(MAGIC_HDR).putInt(nnodes).putInt(depth).putInt(leafSignatureByteLength);
        serializeNode(buffer);

        byte[] serializeTree = new byte[buffer.position()];
        buffer.rewind();
        buffer.get(serializeTree);
        return serializeTree;
    }

    private void serializeNode(ByteBuffer buffer) {
        Queue<Node> queue = new ArrayDeque<>(nnodes / 2 + 1);
        queue.add(root);
        while (!queue.isEmpty()) {
            Node node = queue.remove();
            buffer.put(node.type).putInt(node.signature.length).put(node.signature);
            if (node.getLeft() != null) {
                queue.add(node.getLeft());
            }
            if (node.getRight() != null) {
                queue.add(node.getRight());
            }
        }
    }

    /**
     * Merkle Tree
     */
    @Data
    @Accessors(chain = true)
    public static class Node {

        /**
         * LEAF_SIG_TYPE,INTERNAL_SIG_TYPE
         */
        private byte type;
        private Node left;
        private Node right;
        /**
         * Current node signature
         */
        private byte[] signature;
        private Bucket bucket;

        @Override
        public String toString() {
            return " Node{" + "type=" + type + ",signature=" + Arrays.toString(signature).replace(",", "") + ",left="
                + left + ",right=" + right + '}';
        }

        public boolean signatureEqual(Node other) {
            int length = getSignature().length;
            int length1 = other.getSignature().length;
            if (length != length1) {
                return false;
            }
            for (int i = 0; i < length; i++) {
                if (getSignature()[i] != other.getSignature()[i]) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public String toString() {
        return "MerkleTree{" + "nnodes=" + nnodes + ",depth=" + depth + ",leafSignatureByteLength="
            + leafSignatureByteLength + ",root=" + root + '}';
    }

    public String toSimpleString() {
        return "MerkleTree{" + "nnodes=" + nnodes + ",depth=" + depth + ",leafSignatureByteLength="
            + leafSignatureByteLength + '}';
    }

    interface MerkleConstant {
        int CONSTRUCT_TREE_MIN_SIZE = 2;
        int EVEN_NUMBER = 2;
    }
}
