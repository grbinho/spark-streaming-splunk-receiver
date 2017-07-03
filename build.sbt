name := "spark-streaming-splunk-receiver"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "splunk-artifactory" at "http://splunk.jfrog.io/splunk/ext-releases-local"

libraryDependencies ++= Seq(
  "com.splunk" % "splunk" % "1.6.3.0"
)


//val sparkVersion = "2.1.1"
//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
//  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
//  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
//)

//libraryDependencies += "org.apache.hadoop" % "hadoop-streaming" % "2.7.0"

assemblyJarName in assembly := s"${name.value.replace(' ','-')}-${version.value}.jar"

assemblyOption in assembly := (assemblyOption in assembly).
  value.copy(includeScala = false)