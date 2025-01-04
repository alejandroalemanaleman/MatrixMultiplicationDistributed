package org.example;

import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        DistributedMatrixMultiplication distributedMatrixMultiplication = new DistributedMatrixMultiplication();
        int[][] result = distributedMatrixMultiplication.execute(2000, 2000);
    }
}