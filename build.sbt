name := "carthorse"

version := "0.1"

scalaVersion := "2.10.3"

libraryDependencies += "com.google.guava" % "guava" % "15.0"

libraryDependencies += "com.google.code.findbugs" % "jsr305" % "1.3.+"

libraryDependencies += "com.google.protobuf" % "protobuf-java" % "2.5.0"

libraryDependencies += "io.netty" % "netty" % "3.8.0.Final"

libraryDependencies += "com.stumbleupon" % "async" % "1.4.0"

libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.3.6" exclude("log4j", "log4j") exclude("org.slf4j", "org.slf4j") exclude("jline", "jline") exclude("junit", "junit")

libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.0.13"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.10.1" % "test"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

org.scalastyle.sbt.ScalastylePlugin.Settings
