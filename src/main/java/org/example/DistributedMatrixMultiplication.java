package org.example;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;

public class DistributedMatrixMultiplication {
    static class MatrixMultiplicationTask implements Callable<int[][]>, Serializable {
        private final int[][] chunkA;
        private final int[][] matrixB;

        public MatrixMultiplicationTask(int[][] chunkA, int[][] matrixB) {
            this.chunkA = chunkA;
            this.matrixB = matrixB;
        }

        @Override
        public int[][] call() {
            int rows = chunkA.length;
            int cols = matrixB[0].length;
            int size = matrixB.length;
            int[][] result = new int[rows][cols];

            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < cols; j++) {
                    for (int k = 0; k < size; k++) {
                        result[i][j] += chunkA[i][k] * matrixB[k][j];
                    }
                }
            }
            return result;
        }
    }

    private static int[][] generateMatrix(int rows, int cols) {
        int[][] matrix = new int[rows][cols];
        Random random = new Random();
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = random.nextInt(10);
            }
        }
        return matrix;
    }

    private HazelcastInstance configureHazelcast() {
        Config config = new Config();
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig().setEnabled(false);
        joinConfig.getTcpIpConfig()
                .setEnabled(true)
                .addMember("IP1")
                .addMember("IP2");

        return Hazelcast.newHazelcastInstance(config);
    }

    private boolean isMasterNode(HazelcastInstance hazelcastInstance) {
        return hazelcastInstance.getCluster().getMembers().iterator().next().localMember();
    }

    private void setupMatrices(IMap<Integer, int[][]> distributedMatrixA, IMap<Integer, int[][]> distributedMatrixB, int rows, int cols) {
        System.out.println("Master node detected. Generating matrices...");
        int[][] matrixA = generateMatrix(rows, cols);
        int[][] matrixB = generateMatrix(rows, cols);

        distributedMatrixA.put(0, matrixA);
        distributedMatrixB.put(0, matrixB);

        System.out.println("Matrices have been successfully generated and distributed.");
    }

    private void distributeChunks(IMap<Integer, int[][]> distributedChunks, int[][] matrixA, int chunkSize) {
        List<int[][]> chunks = splitMatrix(matrixA, chunkSize);
        for (int i = 0; i < chunks.size(); i++) {
            distributedChunks.put(i, chunks.get(i));
        }
    }

    private List<int[][]> splitMatrix(int[][] matrixA, int chunkSize) {
        List<int[][]> chunks = new ArrayList<>();
        for (int i = 0; i < matrixA.length; i += chunkSize) {
            int[][] chunk = new int[Math.min(chunkSize, matrixA.length - i)][matrixA[0].length];
            System.arraycopy(matrixA, i, chunk, 0, chunk.length);
            chunks.add(chunk);
        }
        return chunks;
    }

    private int[][] mergeResults(IMap<Integer, int[][]> distributedResults, int rows, int cols, int numChunks) {
        int[][] result = new int[rows][cols];
        int currentRow = 0;

        for (int i = 0; i < numChunks; i++) {
            int[][] partialResult = distributedResults.get(i);
            System.arraycopy(partialResult, 0, result, currentRow, partialResult.length);
            currentRow += partialResult.length;
        }
        return result;
    }

    public int[][] execute(int rows, int cols) throws InterruptedException{
        HazelcastInstance hazelcastInstance = configureHazelcast();

        boolean isMaster = isMasterNode(hazelcastInstance);

        IMap<Integer, int[][]> distributedMatrixA = hazelcastInstance.getMap("matrixA");
        IMap<Integer, int[][]> distributedMatrixB = hazelcastInstance.getMap("matrixB");
        IMap<Integer, int[][]> distributedChunks = hazelcastInstance.getMap("chunks");
        IMap<Integer, int[][]> distributedResults = hazelcastInstance.getMap("results");

        if (isMaster) {
            setupMatrices(distributedMatrixA, distributedMatrixB, rows, cols);
            int[][] matrixA = distributedMatrixA.get(0);
            distributeChunks(distributedChunks, matrixA, 500);
        } else {
            System.out.println("Worker node awaiting tasks...");
        }

        while (!distributedChunks.containsKey(0) || !distributedMatrixB.containsKey(0)) {
            Thread.sleep(100);
        }

        int[][] matrixB = distributedMatrixB.get(0);
        for (int i = 0; i < distributedChunks.size(); i++) {
            if (!distributedResults.containsKey(i)) {
                int[][] chunk = distributedChunks.get(i);
                int[][] partialResult = new MatrixMultiplicationTask(chunk, matrixB).call();
                distributedResults.put(i, partialResult);
            }
        }

        if (isMaster) {
            System.out.println("Master node aggregating results...");
            int[][] matrixA = distributedMatrixA.get(0);
            return mergeResults(distributedResults, matrixA.length, matrixB[0].length, distributedChunks.size());
        } else {
            System.out.println("Worker node completed assigned tasks.");
            return null;
        }
    }
}
