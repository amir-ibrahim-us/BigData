name := "FlinkWordCount"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/org.apache.flink/flink-streaming-java
libraryDependencies += "org.apache.flink" %% "flink-streaming-java" % "1.7.2" % "provided"

// https://mvnrepository.com/artifact/org.apache.flink/flink-streaming-scala
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.7.2"
