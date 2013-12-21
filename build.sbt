import AssemblyKeys._ 

assemblySettings

net.virtualvoid.sbt.graph.Plugin.graphSettings

name := "bulu"

version := "0.1"

scalaVersion := "2.10.1"

resolvers ++= Seq(
	"Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
	"ClouderaRepo" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
	"Pentaho" at "http://repository.pentaho.org/artifactory/pentaho",
	"Thrift" at "http://people.apache.org/~rawson/repo/",
	"lessis" at "http://repo.lessis.me",
  	"coda" at "http://repo.codahale.com",
  	Resolver.url("sbt-plugin-releases", new URL("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-relea"))(Resolver.ivyStylePatterns)
)

libraryDependencies ++= Seq(
	"ch.qos.logback" 			% "logback-classic" 			% "1.0.12" 		% "runtime" ,
	"org.scalatest" 			% "scalatest_2.10" 				% "1.9.1" 		% "test" withSources() withJavadoc(),
	"com.typesafe.akka" 		%% "akka-actor" 				% "2.1.4" withSources() withJavadoc()   ,
	"com.typesafe.akka" 		%% "akka-slf4j" 				% "2.1.4"  ,
	"com.typesafe.akka" 		%% "akka-cluster-experimental" 	% "2.1.4" withSources() withJavadoc() ,
	"com.typesafe.akka" 		%% "akka-contrib" 				% "2.1.4" withSources() withJavadoc() ,
	"org.apache.hadoop" 		% "hadoop-core" 				% "2.0.0-mr1-cdh4.2.1" exclude("org.slf4j","slf4j-log4j12")  ,
	"org.apache.hadoop" 		% "hadoop-common" 				% "2.0.0-cdh4.2.1" exclude("org.slf4j","slf4j-log4j12")  withSources() withJavadoc(),
    "org.apache.hbase" 			% "hbase" 						% "0.94.2-cdh4.2.1" exclude("org.slf4j","slf4j-log4j12") ,
    "org.apache.hive" 			% "hive-jdbc" 					% "0.10.0-cdh4.2.1" exclude("org.slf4j","slf4j-log4j12") 
  )

