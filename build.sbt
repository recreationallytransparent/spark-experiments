name := "pickard-experiments-spark"

version := "0.0.1"

scalaVersion := "2.11.8"

resolvers += Resolver.bintrayRepo("cakesolutions", "maven")

lazy val sparkVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
