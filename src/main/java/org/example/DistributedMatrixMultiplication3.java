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

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;

public class DistributedMatrixMultiplication3 {
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

    private static int[][] generateMatrix(int rows, int cols) {
        int[][] matrix = new int[rows][cols];
        Random random = new Random();
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = random.nextInt(10); // Números aleatorios entre 0 y 9
            }
        }
        return matrix;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // Configuración de Hazelcast
        Config config = new Config();
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig().setEnabled(false);
        joinConfig.getTcpIpConfig()
                .setEnabled(true)
                .addMember("192.168.1.101") // IP del nodo principal
                .addMember("192.168.1.102"); // IP del nodo secundario

        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        // Identificar si este nodo es el principal
        boolean isMaster = hazelcastInstance.getCluster().getMembers().iterator().next().localMember();

        IMap<Integer, int[][]> distributedMatrixA = hazelcastInstance.getMap("matrixA");
        IMap<Integer, int[][]> distributedMatrixB = hazelcastInstance.getMap("matrixB");

        if (isMaster) {
            System.out.println("Soy el nodo principal. Generando matrices...");
            int[][] matrixA = generateMatrix(2000, 2000);
            int[][] matrixB = generateMatrix(2000, 2000);

            distributedMatrixA.put(0, matrixA);
            distributedMatrixB.put(0, matrixB);

            System.out.println("Matrices generadas y distribuidas.");
        } else {
            System.out.println("Soy un nodo secundario. Esperando matrices...");
            // Esperar hasta que las matrices estén disponibles
            while (!distributedMatrixA.containsKey(0) || !distributedMatrixB.containsKey(0)) {
                Thread.sleep(100);
            }
        }

        // Acceder a las matrices distribuidas
        int[][] matrixA = distributedMatrixA.get(0);
        int[][] matrixB = distributedMatrixB.get(0);

        // Realizar la multiplicación de matrices como tarea distribuida
        ExecutorService executorService = hazelcastInstance.getExecutorService("matrixExecutor");
        Future<int[][]> futureResult = executorService.submit(new MatrixMultiplicationTask(matrixA, matrixB));

        int[][] result = futureResult.get();

        System.out.println("Multiplicación completada. Resultados parciales:");
        for (int i = 0; i < Math.min(5, result.length); i++) { // Imprimir solo las primeras 5 filas
            for (int j = 0; j < Math.min(5, result[i].length); j++) {
                System.out.print(result[i][j] + " ");
            }
            System.out.println();
        }

        hazelcastInstance.shutdown();
    }
}
