package org.example;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        DistributedMatrixMultiplication distributedMatrixMultiplication = new DistributedMatrixMultiplication();
        distributedMatrixMultiplication.execute(5000, 5000);
    }
}