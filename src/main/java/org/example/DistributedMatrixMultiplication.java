package org.example;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

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
                .addMember("192.168.1.101")
                .addMember("192.168.1.102");

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

    private void waitForMatrices(IMap<Integer, int[][]> distributedMatrixA, IMap<Integer, int[][]> distributedMatrixB) throws InterruptedException {
        System.out.println("Worker node detected. Awaiting matrices from the master node...");
        while (!distributedMatrixA.containsKey(0) || !distributedMatrixB.containsKey(0)) {
            Thread.sleep(100);
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

    private int[][] mergeResults(List<Future<int[][]>> futures, int rows, int cols) throws ExecutionException, InterruptedException {
        int[][] result = new int[rows][cols];
        int currentRow = 0;

        for (Future<int[][]> future : futures) {
            int[][] partialResult = future.get();
            System.arraycopy(partialResult, 0, result, currentRow, partialResult.length);
            currentRow += partialResult.length;
        }
        return result;
    }

    public int[][] execute(int rows, int cols) throws ExecutionException, InterruptedException {
        HazelcastInstance hazelcastInstance = configureHazelcast();

        boolean isMaster = isMasterNode(hazelcastInstance);

        IMap<Integer, int[][]> distributedMatrixA = hazelcastInstance.getMap("matrixA");
        IMap<Integer, int[][]> distributedMatrixB = hazelcastInstance.getMap("matrixB");

        if (isMaster) {
            setupMatrices(distributedMatrixA, distributedMatrixB, rows, cols);
        } else {
            waitForMatrices(distributedMatrixA, distributedMatrixB);
        }

        int[][] matrixA = distributedMatrixA.get(0);
        int[][] matrixB = distributedMatrixB.get(0);

        List<int[][]> chunks = splitMatrix(matrixA, 500);

        ExecutorService executorService = hazelcastInstance.getExecutorService("matrixExecutor");
        List<Future<int[][]>> futures = new ArrayList<>();

        for (int[][] chunk : chunks) {
            futures.add(executorService.submit(new MatrixMultiplicationTask(chunk, matrixB)));
        }

        // All nodes, including master, compute their assigned chunks
        int[][] localResult = mergeResults(futures, matrixA.length, matrixB[0].length);

        // Combine all local results in master (if needed)
        if (isMaster) {
            System.out.println("Master node aggregating results...");
            return localResult; // In distributed scenarios, master could aggregate remote results too.
        }

        return localResult;
    }
}
