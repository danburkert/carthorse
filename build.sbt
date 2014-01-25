organization := "danburkert"

name := "carthorse"

version := "0.1"

scalaVersion := "2.10.3"

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies += "danburkert" %% "continuum" % "0.4-SNAPSHOT"

//libraryDependencies += "org.apache.hbase" % "hbase-common" % "0.96.1-hadoop2"
//
//libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.2.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.4.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "2.0.0-mr1-cdh4.4.0"

libraryDependencies += "org.apache.hbase" % "hbase" % "0.94.6-cdh4.4.0"

libraryDependencies += "org.kiji.schema" % "kiji-schema" % "1.3.4"

libraryDependencies += "com.chuusai" % "shapeless" % "2.0.0-M1" cross CrossVersion.full

libraryDependencies += "com.google.guava" % "guava" % "15.0"

libraryDependencies += "com.google.code.findbugs" % "jsr305" % "1.3.+"

libraryDependencies += "com.google.protobuf" % "protobuf-java" % "2.5.0"

libraryDependencies += "io.netty" % "netty" % "3.8.0.Final"

libraryDependencies += "com.stumbleupon" % "async" % "1.4.0"

libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.3.6" notTransitive()

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.13"

libraryDependencies += "log4j" % "log4j" % "1.2.15" notTransitive()

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.10.1" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0" % "test"

resolvers += "kiji-repos" at "https://repo.wibidata.com/artifactory/kiji"
