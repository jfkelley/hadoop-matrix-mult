name := "hadoop-matrix-mult"

version := "1.1"

scalaVersion := "2.11.6"

resolvers += "HDP Releases" at "http://repo.hortonworks.com/content/repositories/releases/"

libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.4.0" % "compile"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.4.0" % "compile"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.2" % "test"
libraryDependencies += "junit" % "junit" % "4.8.1" % "test"
libraryDependencies += "org.apache.mrunit" % "mrunit" % "1.1.0" % "test" classifier "hadoop2"