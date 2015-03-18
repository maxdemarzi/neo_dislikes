Neo4j Dislikes Unmanaged Extension
=============================================

Test two different ways to model dislikes

1. Is using a traditional "DISLIKES" relationship to items a user does not want.
2. Is using a [RoaringBitmap](http://roaringbitmap.org/) to store just the node ids of items a user does not want.


Install the [IntelliJ JMH plugin](https://github.com/artyushov/idea-jmh-plugin)

Open this in Intellij, go to ServiceBenchmark and run it.

