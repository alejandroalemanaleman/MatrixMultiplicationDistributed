# Distributed Execution of Matrix Multiplication

## Prerequisites

Before executing the code, ensure that both nodes are connected to the same network and specify the IP addresses of the nodes in the following section of the code.

In the DistributedMatrixMultiplication class:
```
private HazelcastInstance configureHazelcast() {
        Config config = new Config();
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig().setEnabled(false);
        joinConfig.getTcpIpConfig()
                .setEnabled(true)
                .addMember("IP1")  // Insert the IP of Node 1
                .addMember("IP2"); // Insert the IP of Node 2

        return Hazelcast.newHazelcastInstance(config);
    }
```

## How to Run

Execute the same code on both nodes by running Main.java. To modify the matrix size, update the parameters in the main method (on both nodes):
```
distributedMatrixMultiplication.execute(rows, cols);
```