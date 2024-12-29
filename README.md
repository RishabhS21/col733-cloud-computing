This repository is a fork of my professor's original repository for the course **Cloud Computing Technology Fundamentals**. It contains the complete course materials, including lecture notes, assignments, and projects. The official course webpage can be found [here](https://abhilash-jindal.com/teaching/2024-1-col-733/).  

All the labs for this course are located in the [labs](./labs) directory. Unlike the original repository, this fork includes fully functional code for each lab, along with detailed explanations and relevant documentation within their respective folders.

# Lecture notes 
For COL733: cloud computing technology fundamentals, taught at IIT Delhi.

1. [Introduction](./why-cloud.md): resource management overheads, SLAs,
elasticity, proximity; distributed computations: stragglers, resource
utilization, fault tolerance; virtualization: overprovisioning, snapshot,
migration, isolation; distributed storage: replication for FT, consistency.

### Distributed compute
1. [Scalability](./compute-scalability.md): speedup, efficiency, iso-efficiency,
scalability, task DAGs.
2. [Distributed shared memory](./compute-dsm.md): design of paging-based DSM,
difficulties due to memory coherence, false sharing, replication in DSM for
faults, stragglers.
3. [MapReduce](./compute-mr.md): give up on general purpose programs, restricted
but useful programming model, locality, scalability, rerunning idempotent
deterministic tasks for FT/stragglers.
4. [Spark](./compute-rdd.md): lineage-based reexecution for FT/stragglers,
coarse-grained transformations for small lineage graphs, immutable distributed
data for consistent replication, wide/narrow dependencies.
5. [Spark streaming](./compute-dstreams.md): Freshness objective, continuous
operator model, FT challenges due to stateful operators, micro-batching to make
computations stateless.
6. [Vector clocks](./compute-vc.md): consistent asynchronous checkpointing
algorithm, inconsistent checkpoints due to clock drifts, vector clocks,
isomorphism with causality.
7. [Flink](./compute-flink.md): Flink's asynchronous consistent checkpointing
algorithm enabling real-time stateful streaming.
8. [TensorFlow Part I](./compute-tf-graph.md): Requirements from ML training
workloads, parameter servers, unified dataflow graph.
9. [TensorFlow Part II](./compute-tf): heterogenous execution, leveraging weak
consistency: async parameter updates, inconsistent checkpoints for FT, M/N
updates for stragglers.
10. [Ray](./compute-ray): Dynamic task DAGs using tasks/futures, stateful actor
methods in lineage, distributed scheduler. 

### Distributed storage
1. [Google File System](./storage-gfs.md): Distributed storage challenges, file
system interface for large files, GFS design, weak consistency guarantees, FT of
chunkservers/master.
2. [Chain Replication with Apportioned Queries](./storage-craq.md): safety/liveness,
linearizability, CRAQ read throughput improvements over chain replication, chain
replication can replicate state machines.
3. [Dynamo](./storage-dynamo.md): CAP theorem, decentralized storage design,
consistent hashing, gossip protocol using vector clocks, sloppy quorums with
hinted replicas, version reconciliations.
4. [Bayou/CRDT](./storage-ec.md): State-based CRDTs: monotonic operations on a 
semilattice; Operations-based CRDTs: commutative operations; Bayou for handling
conflicting writes by putting them in a replicated log; anti-entropy; eventual
consistency.
5. [Raft](./storage-raft.md): Raft's safety properties: state-machine safety,
election safety, leader completeness property; liveness properties: majority
within bounded delay implies progress; automated failover via leader election:
idea of majority, consensus, FLP impossibility. 
6. [Zookeeper](./storage-zookeeper.md): Improving read throughput of Raft-like
consensus system by allowing stale reads, Zookeeper API: znodes, ephemeral and 
sequential znodes, watches; usecases: configuration management, rendezvous,
group membership, distributed locking; distributed locks: herd effect, using
fencing tokens.
7. [Spanner](./storage-spanner.md): Serializability, strict serializability,
optimistic concurrency control (CC), pessimistic CC, multi-version CC,
wound-wait deadlock avoidance, two phase commits, lock-free snapshot reads,
commit/start timestamps, safe time, clock skew, commit wait, true time API,
time masters.

### Virtualization
1. [CPU virtualization](./virt-cpu.md): Operational advantages of
virtualization: overprovisioning, strong isolation, reduced trusted computing
base, snapshot/restore, live migration; OS background: privilege levels, limited
direct execution, trap handling, system calls; trap-and-emulate hypervisors: 
Popek-Goldberg theorem, problematic x86 instructions, full virtualization with
dynamic binary translation (VMWare), paravirtualization (Xen), hardware-assisted
virtualization (KVM).
2. [Memory virtualization](./virt-mem.md): OS background: virtual address space,
virtual-to-physical address translation using paging hardware, page table entries,
page faults; two-level page tables, extended page tables, memory ballooning,
kernel samepage merging.
3. [IO virtualization](./virt-io.md) : OS background: port IO, memory mapped IO,
direct memory access, ring buffers; IO emulation: trap on PIO and MMIO,
unnecessary VM exits since physical devices were not designed with
virtualization goal; IO paravirtualization: reduce VM exits; HW assisted IO
virtualization: SRIOV, IOMMU.

# Labs
1. [Map-reduce style computation](./labs/lab1): In this lab, built a distributed word-count application using Python and Redis. Workers processed tasks concurrently by reading file names from Redis streams, counting word frequencies, and storing results in a Redis sorted set. We ensured fault tolerance for worker crashes by redistributing unacknowledged tasks and making Redis updates atomic. Additionally, we implemented Redis fault tolerance by creating periodic checkpoints for recovery. This lab demonstrated key distributed system concepts like parallelism and fault tolerance.
2. [Lightweight asynchronous consistent checkpointing](./labs/lab2): Implemented Chandy-Lamport Algorithm on a Flink-like setting for consistent global checkpointing of the system and used these checkpoints to recover from worker failures.
3. [Chain Replication using CRAQ](./labs/lab3): Implemented strong consistency variant of CRAQ: A Distibuted Object-Storage System from scratch and improved the read throughput as compared to the basic implementation of Chain Replication that we had already provided where I ensured the read-write history is linearizable.
4. [Bayou](./labs/lab4): Implemented a simplified version of [Bayou](https://people.eecs.berkeley.edu/~brewer/cs262b/update-conflicts.pdf) which is a weakly connected replicated storage system.

# Project
As the course project, we developed a simplified, toy version of Amazon Dynamo, Amazon's highly available key-value store. The implementation is available on my GitHub [here](https://github.com/RishabhS21/Dynamo-Amazon-s-Highly-Available-Key-value-Store).
