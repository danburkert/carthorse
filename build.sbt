organization := "danburkert"

name := "carthorse"

version := "0.1"

scalaVersion := "2.10.3"

libraryDependencies += "danburkert" %% "continuum" % "0.2-SNAPSHOT"

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

org.scalastyle.sbt.ScalastylePlugin.Settings
