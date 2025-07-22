## worker pool:

### 1/10k
Pipelines created: 1
Total messages processed: 10000
Pipeline creation: 29.405 ms
Plugin setup: 16.8 ms
Message sending: 18.141 ms
Message processing: 253.284 ms
Total time: 317.63 ms
Throughput: 39481.37 messages/second
Average time per pipeline: 317.63 ms

### 10/10k
Pipelines created: 10
Total messages processed: 100000
Pipeline creation: 41.22 ms
Plugin setup: 13.765 ms
Message sending: 324.687 ms
Message processing: 1432.709 ms
Total time: 1812.381 ms
Throughput: 69797.84 messages/second
Average time per pipeline: 181.2381 ms

### 100/10k
Pipelines created: 100
Total messages processed: 1000000
Pipeline creation: 129.656 ms
Plugin setup: 23.057 ms
Message sending: 2811.538 ms
Message processing: 18302.149 ms
Total time: 21266.4 ms
Throughput: 54638.39 messages/second
Average time per pipeline: 212.664 ms

### 10/100k
Pipelines created: 10
Total messages processed: 1000000
Pipeline creation: 36.673 ms
Plugin setup: 21.923 ms
Message sending: 3418.824 ms
Message processing: 19198.176 ms
Total time: 22675.596 ms
Throughput: 52088.28 messages/second
Average time per pipeline: 2267.5596 ms

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