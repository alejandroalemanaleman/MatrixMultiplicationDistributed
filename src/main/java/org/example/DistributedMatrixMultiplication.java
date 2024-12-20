package org.example;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

import java.io.Serializable;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class DistributedMatrixMultiplication {

    // Callable task for multiplying two matrix chunks
    static class MatrixMultiplicationTask implements Callable<int[][]>, Serializable {
        private final int[][] chunkA;
        private final int[][] chunkB;

        public MatrixMultiplicationTask(int[][] chunkA, int[][] chunkB) {
            this.chunkA = chunkA;
            this.chunkB = chunkB;
        }

        @Override
        public int[][] call() {
            int rows = chunkA.length;
            int cols = chunkB[0].length;
            int size = chunkB.length;
            int[][] result = new int[rows][cols];

            for (int i = 0; i < rows; i++) {
                for (int j = 0; j < cols; j++) {
                    for (int k = 0; k < size; k++) {
                        result[i][j] += chunkA[i][k] * chunkB[k][j];
                    }
                }
            }
            return result;
        }
    }

    private static double[][] generateMatrix(int rows, int cols) {
        double[][] matrix = new double[rows][cols];
        Random random = new Random();
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = random.nextDouble() * 10;
            }
        }
        return matrix;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Start Hazelcast instance
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

        // Example Matrices
        int[][] matrixA = {
                {1, 2, 3},
                {4, 5, 6}
        };
        int[][] matrixB = {
                {7, 8},
                {9, 10},
                {11, 12}
        };

        // Partition the matrices into smaller chunks
        // Here we use the entire matrices for simplicity
        IMap<Integer, int[][]> distributedMatrixA = hazelcastInstance.getMap("matrixA");
        IMap<Integer, int[][]> distributedMatrixB = hazelcastInstance.getMap("matrixB");

        distributedMatrixA.put(0, matrixA);
        distributedMatrixB.put(0, matrixB);

        // Submit tasks for computation
        ExecutorService executorService = hazelcastInstance.getExecutorService("matrixExecutor");
        Future<int[][]> futureResult = executorService.submit(new MatrixMultiplicationTask(matrixA, matrixB));

        // Combine results
        int[][] result = futureResult.get();

        // Print result
        System.out.println("Resultant Matrix:");
        for (int[] row : result) {
            for (int val : row) {
                System.out.print(val + " ");
            }
            System.out.println();
        }

        // Shutdown Hazelcast instance
        hazelcastInstance.shutdown();
    }
}
