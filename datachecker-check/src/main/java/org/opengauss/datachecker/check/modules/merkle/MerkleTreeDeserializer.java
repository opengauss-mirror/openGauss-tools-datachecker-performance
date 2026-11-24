/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-tools-datachecker-performance is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.check.modules.merkle;

import org.opengauss.datachecker.check.modules.merkle.MerkleTree.Node;
import org.opengauss.datachecker.common.util.ByteUtil;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Merkel tree deserialization
 * bucket Bucket deserialization implementation
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class MerkleTreeDeserializer {

    /**
     * Deserialization is implemented according to the byte array returned by {@link MerkleTree#serialize()}
     * Serialization format ：
     * header (magic header:int)(num nodes:int)(tree depth:int)(leaf length:int)
     * [(node type:byte)(signature length:int)(signature:byte)]
     *
     * @param serializerTree
     * @return
     */
    public static MerkleTree deserialize(byte[] serializerTree) {
        ByteBuffer buffer = ByteBuffer.wrap(serializerTree);

        // Byte array header verification
        if (buffer.getInt() != MerkleTree.MAGIC_HDR) {
            throw new IllegalArgumentException("Serialized byte array does not start with a legal magic header");
        }
        // Read header information
        int totalNodes = buffer.getInt();
        int depth = buffer.getInt();
        int leafLength = buffer.getInt();

        // Read the root node
        Node root = new Node().setType(buffer.get()).setSignature(readNextSingature(buffer));
        if (root.getType() == MerkleTree.LEAF_SIG_TYPE) {
            throw new IllegalArgumentException("The first serialized node is a leaf node");
        }

        Queue<Node> queue = new ArrayDeque<>(totalNodes / 2 + 1);

        Node currentNode = root;
        for (int i = 1; i < totalNodes; i++) {
            Node child = new Node().setType(buffer.get()).setSignature(readNextSingature(buffer));
            queue.add(child);
            // Handle the incomplete tree that the node has been promoted:
            // (if the signatures of currentnode and child node are consistent)
            if (ByteUtil.isEqual(currentNode.getSignature(), child.getSignature())) {
                currentNode.setLeft(child);
                currentNode = queue.remove();
                continue;
            }
            if (currentNode.getLeft() == null) {
                currentNode.setLeft(child);
            } else {
                currentNode.setRight(child);
                currentNode = queue.remove();
            }

        }
        return new MerkleTree(root, totalNodes, depth, leafLength);
    }

    private static byte[] readNextSingature(ByteBuffer buffer) {
        byte[] singatureBytes = new byte[buffer.getInt()];
        buffer.get(singatureBytes);
        return singatureBytes;
    }
}
