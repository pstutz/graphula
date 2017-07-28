resolvers in ThisBuild += Resolver.sonatypeRepo("snapshots")

// Eclipse settings
EclipseKeys.withSource := true
EclipseKeys.createSrc := EclipseCreateSrc.Default + EclipseCreateSrc.ManagedClasses

scalacOptions in ThisBuild ++= Seq("-unchecked", "-deprecation", "-feature")

lazy val core = project
  .in(file("core"))
  .settings(sharedSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "net.jpountz.lz4" % "lz4" % "1.3.0",
      "org.lmdbjava" % "lmdbjava" % "0.0.5",
      "com.google.guava" % "guava" % "21.0"
    )
  )

lazy val sparql = project
  .in(file("sparql"))
  .settings(sharedSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.jena" % "jena-arq" % "3.3.0",
      "org.apache.jena" % "jena-core" % "3.3.0",
      "org.semanticweb.yars" % "nxparser-parsers" % "2.3.3",
      "org.slf4j" % "slf4j-nop" % "1.7.25",
      "org.eclipse.rdf4j" % "rdf4j-sparql-testsuite" % "2.2.1" % "test",
      "org.apache.jena" % "jena-core" % "3.3.0" % "test" classifier "tests"
    )
  )
  .dependsOn(core)

lazy val benchmarks = project
  .in(file("benchmarks"))
  .settings(sharedSettings: _*)
  .dependsOn(sparql)
  .enablePlugins(JmhPlugin)

lazy val sharedSettings = scalaVersionSettings ++ Seq(
    libraryDependencies ++= testDependencies
  )

lazy val scalaVersionSettings = Seq(
  scalaVersion := "2.12.2"
)

lazy val testDependencies = Seq(
  "org.scalactic" %% "scalactic" % "3.0.3" % "test",
  "org.scalatest" %% "scalatest" % "3.0.3" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.5" % "test"
)
