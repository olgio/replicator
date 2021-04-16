#!/bin/bash
echo Startting script

#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 3 --f 1 --keys 1
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 3 --f 1 --keys 30
#
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 1 --keys 1
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 1 --keys 30
#
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 1
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 30
#
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 3 --keys 1
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 3 --keys 30
#
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 5 --keys 1
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 5 --keys 30

#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol RaftOnlyLeader --n 3 --keys 1
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol RaftOnlyLeader --n 3 --keys 30
#
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol RaftOnlyLeader --n 5 --keys 1
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol RaftOnlyLeader --n 5 --keys 30


#for conflict influence diagram Atlas

#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 1
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 2
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 4
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 5
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 10
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 15
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 20
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 50
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 70
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 100
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 400
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 800
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 1600
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 3200

#for conflict influence diagram Atlas end

#for conflict influence diagram Atlas

java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 5 --f 2 --keys 1
java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 5 --f 2 --keys 5
java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 5 --f 2 --keys 15
java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 5 --f 2 --keys 30
java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 5 --f 2 --keys 100
java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 5 --f 2 --keys 200
java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 5 --f 2 --keys 800
java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 5 --f 2 --keys 3200

#for conflict influence diagram Atlas end

#for latency diagram keys = 30

#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 3 --f 1 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 3 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol RaftOnlyLeader --n 3 --keys 30
#
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 1 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 5 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol RaftOnlyLeader --n 5 --keys 30
#
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 7 --f 1 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 7 --f 2 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 7 --f 3 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 7 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol RaftOnlyLeader --n 7 --keys 30

#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 9 --f 1 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 9 --f 4 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 9 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol RaftOnlyLeader --n 9 --keys 30
#
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 11 --f 1 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 11 --f 5 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 11 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol RaftOnlyLeader --n 11 --keys 30
#
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 13 --f 1 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 13 --f 6 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 13 --keys 30
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol RaftOnlyLeader --n 13 --keys 30

#end for latency diagram

#for latency diagram keys = 3200

#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 3 --f 1 --keys 3200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 3 --keys 3200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol RaftOnlyLeader --n 3 --keys 3200
#
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 1 --keys 3200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 5 --f 2 --keys 3200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 5 --keys 3200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol RaftOnlyLeader --n 5 --keys 3200
#
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 7 --f 1 --keys 3200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 7 --f 2 --keys 3200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 7 --f 3 --keys 3200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 7 --keys 3200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol RaftOnlyLeader --n 7 --keys 3200
#
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 9 --f 1 --keys 3200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 9 --f 4 --keys 3200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 9 --keys 3200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol RaftOnlyLeader --n 9 --keys 3200
#
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 11 --f 1 --keys 3200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 11 --f 5 --keys 3200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 11 --keys 3200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol RaftOnlyLeader --n 11 --keys 3200
#
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 13 --f 1 --keys 3200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Atlas --n 13 --f 6 --keys 3200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol Raft --n 13 --keys 3200
#java -Xms1536M -Xmx1536M -cp /app.jar ru.splite.replicator.benchmark.BenchmarkKt --protocol RaftOnlyLeader --n 13 --keys 3200

#end for latency diagram


echo Completed script