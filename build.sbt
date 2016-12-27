name := "streams"

organization := "io.papacharlie"

version := "0.1"

scalaVersion := "2.11.8"

scalacOptions := Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xlint",
  "-Yno-adapted-args",
  //    "-Ywarn-value-discard", // Useful for debugging
  "-Ywarn-adapted-args",
  "-Ywarn-inaccessible",
  "-Ywarn-infer-any",
  "-Ywarn-nullary-override",
  "-Ywarn-nullary-unit"
)

libraryDependencies ++= Seq(
  "com.amazonaws" % "amazon-kinesis-client" % "1.6.3",
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.9.21",
  "com.twitter" %% "util-core" % "6.40.0",
  "com.twitter" %% "bijection-util" % "0.9.4",

  "org.joda" % "joda-convert" % "1.8.1",

  "org.mockito" % "mockito-core" % "1.10.19" % Test,
  "org.specs2" %% "specs2-core" % "3.8.3" % Test,
  "org.specs2" %% "specs2-mock" % "3.8.3" % Test
)

javacOptions in(Compile, compile) ++= Seq(
  "-source", "1.8",
  "-target", "1.8",
  "-Xlint:unchecked"
)

checksums := Seq("sha1")

fork in(Compile, run) := true

fork in Test := true
