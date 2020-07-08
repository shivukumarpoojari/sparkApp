name := "SparkApp"
version := "0.1"

scalaVersion := "2.12.10"
autoScalaLibrary:=false
val sparkVersion = "3.0.0"
resolvers += "MavenRepository" at "https://mvnrepository.com"
val sparkDependencies= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

val testDependencies=Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)
libraryDependencies ++=sparkDependencies ++ testDependencies
//addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.1")
updateOptions := updateOptions.value.withCachedResolution(true)
ThisBuild / useCoursier := false





