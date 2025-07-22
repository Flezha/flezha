## worker pool:

### 1/10k
Pipelines created: 1
Total messages processed: 10000
Pipeline creation: 12.482 ms
Plugin setup: 15.331 ms
Message sending: 10.855 ms
Message processing: 175.2 ms
Total time: 213.868 ms
Throughput: 57077.63 messages/second
Average time per pipeline: 213.868 ms

### 10/10k
Pipelines created: 10
Total messages processed: 100000
Pipeline creation: 16.686 ms
Plugin setup: 11.629 ms
Message sending: 137.918 ms
Message processing: 973.152 ms
Total time: 1139.385 ms
Throughput: 102758.87 messages/second
Average time per pipeline: 113.9385 ms

### 100/10k
Pipelines created: 100
Total messages processed: 1000000
Pipeline creation: 68.781 ms
Plugin setup: 8.507 ms
Message sending: 1317.926 ms
Message processing: 10183.557 ms
Total time: 11578.771 ms
Throughput: 98197.52 messages/second
Average time per pipeline: 115.78771 ms

### 10/100k
Pipelines created: 10
Total messages processed: 1000000
Pipeline creation: 17.971 ms
Plugin setup: 5.721 ms
Message sending: 1378.278 ms
Message processing: 11435.233 ms
Total time: 12837.203 ms
Throughput: 87449.03 messages/second
Average time per pipeline: 1283.7203 ms

## old impl:

### 1/10k
Pipelines created: 1
Total messages processed: 10000
Pipeline creation: 23.52 ms
Plugin setup: 10.569 ms
Message sending: 8.526 ms
Message processing: 193.497 ms
Total time: 236.112 ms
Throughput: 51680.39 messages/second
Average time per pipeline: 236.112 ms

### 10/10k
Pipelines created: 10
Total messages processed: 100000
Pipeline creation: 34.652 ms
Plugin setup: 17.241 ms
Message sending: 150.668 ms
Message processing: 995.062 ms
Total time: 1197.623 ms
Throughput: 100496.25 messages/second
Average time per pipeline: 119.7623 ms

### 100/10k
Pipelines created: 100
Total messages processed: 1000000
Pipeline creation: 76.869 ms
Plugin setup: 8.327 ms
Message sending: 1818.912 ms
Message processing: 9188.834 ms
Total time: 11092.942 ms
Throughput: 108827.74 messages/second
Average time per pipeline: 110.92942 ms

### 10/100k
Pipelines created: 10
Total messages processed: 1000000
Pipeline creation: 33.266 ms
Plugin setup: 10.892 ms
Message sending: 1235.551 ms
Message processing: 10631.633 ms
Total time: 11911.342 ms
Throughput: 94058.93 messages/second
Average time per pipeline: 1191.1342 ms