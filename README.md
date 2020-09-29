# Deadlock in Graph with Partition/Merge Nested in Broadcast/Zip

Full code and test case for the issue described in https://discuss.lightbend.com/t/deadlock-in-graph-with-partition-merge-nested-in-broadcast-zip/7264
The sample also shows that a simpler implementation of the case statement, which consists only of the Partion/merge block, does not exhibit the deadlock, but it emits the elements out of order. 

To build and run, use `.gradlew clean test`
