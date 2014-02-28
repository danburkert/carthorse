organization := "danburkert"

name := "carthorse"

version := "0.1"

scalaVersion := "2.10.3"

scalacOptions += "-feature"

resolvers += Resolver.sonatypeRepo("snapshots")

resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"

libraryDependencies += "danburkert" %% "continuum" % "0.4-SNAPSHOT"

libraryDependencies += "org.apache.hbase" % "hbase-common" % "0.96.1-hadoop2"

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.2.0"

libraryDependencies += "org.apache.curator" % "curator-recipes" % "2.4.0"

libraryDependencies += "org.apache.curator" % "curator-test" % "2.4.0"

libraryDependencies += "com.chuusai" % "shapeless" % "2.0.0-M1" cross CrossVersion.full

libraryDependencies += "com.google.guava" % "guava" % "16.0.1"

libraryDependencies += "com.google.code.findbugs" % "jsr305" % "1.3.+"

libraryDependencies += "org.hbase" % "asynchbase" % "1.5.1-SNAPSHOT"

libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.10.1" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.0" % "test"

seq( sbtavro.SbtAvro.avroSettings : _*)

(stringType in avroConfig) := "String"

(version in avroConfig) := "1.7.5"
