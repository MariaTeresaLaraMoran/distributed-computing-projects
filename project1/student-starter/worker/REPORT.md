# Performance Evaluation

## Method 1 — Test Apparatus Performance Panel

The Test Apparatus provides a real-time performance panel that measures **end-to-end system latency**.  
This latency represents the total time from when a message is sent to the input SQS queue until the result is received by the browser.

The measurement includes several components of the pipeline:

SQS message delivery → Lambda execution → results queue → browser polling.
DynamoDB writes occur after the result is sent and are not part of the latency-critical path.

Warmup runs were executed first to initialize Lambda containers and reduce cold start effects.  
After that, several workload profiles (Steady, Burst, Ramp, and Soak) were executed to measure system behavior under different traffic conditions.

### Experimental Results

| Test | Messages | Avg Latency (ms) | Min (ms) | p95 (ms) | Max (ms) | Results/sec |
|-----|----------|------------------|---------|---------|---------|-------------|
| Warmup 1 | 10 | 2577 | 1927 | 3654 | 3654 | 2.3 |
| Warmup 2 | 10 | 2222 | 1335 | 2842 | 2842 | 2.8 |
| Steady | 100 | 2565 | 447 | 5290 | 6311 | 11.2 |
| Burst | 500 | 7390 | 759 | 14358 | 15670 | 19.4 |
| Ramp 1 | 500 | 12067 | 1081 | 21074 | 22279 | 18.5 |
| Ramp 2 | 500 | 8724 | 1456 | 16672 | 17753 | 19.1 |
| Soak | 200 | 16896 | 369 | 26463 | 28321 | 5.1 |

### Additional Testing — Post-Debug Validation

After debugging and validating the Lambda implementation, an additional Burst test was executed to confirm system performance under a clean run.

| Test | Messages | Avg Latency (ms) | Min (ms) | p95 (ms) | Max (ms) | Results/sec |
|-----|----------|------------------|---------|---------|---------|-------------|
| Burst (Post-Debug) | 500 | 6195 | 652 | 11047 | 12114 | 36.8 |

### Observations

The results show that system throughput increases as workload increases.  
During the Warmup phase, throughput is low **2.8 results per second** because Lambda containers are just initializing.  
Under the Steady workload, throughput increases to about **11 results per second**.
When larger workloads are introduced in the Burst and Ramp tests, AWS Lambda automatically scales additional workers. In the initial tests, throughput stabilized at approximately **18–19 results per second**, while the post-debug Burst test shows improved throughput of **36.8 results per second**, demonstrating the elasticity and scalability of the serverless architecture.
Although average latency increases under high load, this metric includes queue waiting time and browser polling delays. These values therefore represent **end-to-end system latency**, not the internal execution time of the Lambda function.

Notably, the minimum observed latency was **369 ms**, which indicates that the system can achieve sub-500 ms latency when containers are warm and messages are processed immediately.
The post-debug Burst test shows a significant increase in throughput (36.8 results/sec compared to 19.4 previously), indicating that the system is processing messages more efficiently and scaling effectively under load. 
Although average latency remains high, this is expected in burst scenarios where all messages are sent simultaneously, causing queueing delays. This result reinforces that while system optimizations can improve throughput, end-to-end latency is still largely influenced by the time messages spend waiting in the queue rather than the Lambda execution itself.

Overall, these results confirm that the system scales effectively under increased load, and that performance is primarily constrained by queueing dynamics rather than computation efficiency.

---
## Method 2 — CloudWatch Logs (Lambda Internal Timing)

CloudWatch logs were used to measure **Lambda internal execution time**, independent of network round-trip latency.  
These logs provide visibility into the execution time of the function itself and help distinguish between Lambda processing time and external system delays.

The logs include several useful timing metrics:

- **Processed `<id>` in X ms** – processing time for a single opportunity.
- **Batch complete** – total time to process a batch of messages.
- **REPORT Duration** – total Lambda execution time.
- **Init Duration** – container initialization time (cold start).

### CloudWatch Observations

| Metric | Observed Value |
|------|----------------|
| Cold start initialization | ~497 ms |
| Lambda execution duration (warm container) | ~400–500 ms |
| Memory allocated | 256 MB |
| Maximum memory used | 90 MB |

### Interpretation

The CloudWatch logs show that the Lambda cold start initialization time is approximately **497 ms**, which is typical for Python-based Lambda functions.  

Once the container is initialized, subsequent invocations run on **warm containers**, and execution times are typically **under 500 ms**, meeting the performance target specified in the assignment.

The logs also indicate efficient memory usage, with the function using only **90 MB of the allocated 256 MB**.

### Key Insight

The CloudWatch measurements confirm that the Lambda implementation meets the expected performance target of **sub-500 ms execution time on warm containers**, while the higher latency observed in Method 1 is primarily caused by queueing delays and browser polling intervals.

The difference between Method 1 and Method 2 highlights a key principle of distributed systems:

- Method 1 measures **end-to-end system latency**, which includes queue delays, Lambda invocation time, and client-side polling.
- Method 2 measures **Lambda execution time only**, isolating the performance of the compute component.

The results show that while the Lambda function itself performs efficiently (sub-500 ms), the overall system latency is dominated by external factors such as SQS queueing delays and scaling behavior. This demonstrates that in distributed architectures, system-level performance is often constrained more by coordination and communication overhead than by computation time.

As workload increases, average latency rises significantly due to queueing effects. When many messages arrive simultaneously, they accumulate in the SQS queue and wait for available Lambda workers. Although AWS Lambda scales automatically, this scaling is not instantaneous, leading to temporary backlogs and increased waiting time. This explains the higher latency observed in Burst, Ramp, and Soak tests.

