import scala.sys.process._
import scala.scalanative.build._
import sbtcrossproject.CrossPlugin.autoImport._
import sbtassembly.AssemblyPlugin.autoImport._

val Scala3 = "3.3.6"

val airframeLogVersion = "2025.1.12"
val circeVersion = "0.14.14"
val circeOpticsVersion = "0.15.1"
val jsonPathVersion = "0.2.0"
val specs2Version = "4.23.0"
val fastparseVersion = "3.1.1"
val javaJqVersion = "2.0.0"
val scribeVersion = "3.17.0"
val monocleVersion = "3.3.0"
val mainArgsVersion = "0.7.7"

ThisBuild / scalafmtOnCompile := true
ThisBuild / name := "Sequala"
ThisBuild / version := "2.0.0-M8"
ThisBuild / organization := "dev.mauch"
ThisBuild / scalaVersion := Scala3
ThisBuild / crossScalaVersions := Seq(Scala3)

val scalacheckVersion = "1.19.0"
val testcontainersVersion = "0.44.0"

val schemaModuleSettings = Seq(
  libraryDependencies ++= Seq(
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-generic-extras" % "0.14.5-RC1",
    "org.wvlet.airframe" %% "airframe-log" % airframeLogVersion,
    "org.specs2" %% "specs2-core" % specs2Version % "test",
    "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test"
  ),
  publishMavenStyle := true,
  pomExtra := <url>http://github.com/nightscape/sequala/</url>
    <licenses>
      <license>
        <name>Apache License 2.0</name>
        <url>http://www.apache.org/licenses/</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:nightscape/sequala.git</url>
      <connection>scm:git:git@github.com:nightscape/sequala.git</connection>
    </scm>,
)

val parserModuleSettings = Seq(
  libraryDependencies ++= Seq(
    "com.lihaoyi" %% "fastparse" % fastparseVersion,
    "com.outr" %% "scribe" % scribeVersion,
    "org.wvlet.airframe" %% "airframe-log" % airframeLogVersion,
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-parser" % circeVersion,
    "org.specs2" %% "specs2-core" % specs2Version % "test",
    "org.specs2" %% "specs2-junit" % specs2Version % "test",
    "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test"
  ),
  publishMavenStyle := true,
  pomExtra := <url>http://github.com/nightscape/sequala/</url>
    <licenses>
      <license>
        <name>Apache License 2.0</name>
        <url>http://www.apache.org/licenses/</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:nightscape/sequala.git</url>
      <connection>scm:git:git@github.com:nightscape/sequala.git</connection>
    </scm>,
)

val nativeSettings = Seq(
  nativeConfig ~= {
    _.withLTO(LTO.full)
      .withMode(Mode.releaseFast)
      .withGC(GC.commix)
  }
)

// =============================================================================
// Schema modules (no parsing dependencies)
// =============================================================================

lazy val schema =
  project
  .in(file("schema"))
  .settings(
    name := "sequala-schema",
    schemaModuleSettings,
  )

lazy val schemaOracle =
  project
  .in(file("schema-oracle"))
  .dependsOn(schema)
  .settings(
    name := "sequala-schema-oracle",
    schemaModuleSettings,
  )

lazy val schemaPostgres =
  project
  .in(file("schema-postgres"))
  .dependsOn(schema)
  .settings(
    name := "sequala-schema-postgres",
    schemaModuleSettings,
  )

// =============================================================================
// Parser modules
// =============================================================================

lazy val parser =
  project
  .in(file("parser"))
  .dependsOn(schema)
  .settings(
    name := "sequala-parser",
    parserModuleSettings,
  )

lazy val parserOracle =
  project
  .in(file("parser-oracle"))
  .dependsOn(parser % "compile->compile;test->test", schemaOracle)
  .settings(
    name := "sequala-parser-oracle",
    parserModuleSettings,
  )

lazy val parserPostgres =
  project
  .in(file("parser-postgres"))
  .dependsOn(parser, schemaPostgres)
  .settings(
    name := "sequala-parser-postgres",
    parserModuleSettings,
  )

// =============================================================================
// CLI module (depends on all parsers for multi-dialect support)
// =============================================================================

lazy val cli =
  project
  .in(file("cli"))
  .dependsOn(parser, parserOracle, parserPostgres, migrate, migratePostgres, migrateOracle, migrate % "test->test")
  .settings(
    name := "sequala-cli",
    parserModuleSettings,
    libraryDependencies ++= Seq(
      "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4",
      "com.lihaoyi" %% "mainargs" % mainArgsVersion,
      "io.circe" %% "circe-generic-extras" % "0.14.5-RC1",
      "io.circe" %% "circe-optics" % circeOpticsVersion,
      "dev.optics" %% "monocle-core" % monocleVersion,
      "dev.optics" %% "monocle-macro" % monocleVersion,
      "com.quincyjo" %% "scala-json-path-circe" % jsonPathVersion,
      "com.arakelian" % "java-jq" % javaJqVersion,
      "io.circe" %% "circe-yaml" % "0.16.0",
      "com.typesafe" % "config" % "1.4.3",
      "org.postgresql" % "postgresql" % "42.7.4",
      "com.oracle.database.jdbc" % "ojdbc11" % "23.6.0.24.10",
      "com.lihaoyi" %% "os-lib" % "0.11.6",
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.scalatestplus" %% "scalacheck-1-18" % "3.2.19.0" % Test,
      "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
    ),
    // Assembly settings
    assembly / mainClass := Some("sequala.Main"),
    assembly / assemblyJarName := s"${name.value}-${version.value}.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    assembly / artifact := {
      val art = (assembly / artifact).value
      art.withClassifier(Some("assembly"))
    },
    addArtifact(assembly / artifact, assembly),
  )
  .enablePlugins(AssemblyPlugin)

// =============================================================================
// Migration module
// =============================================================================

lazy val migrate =
  project
  .in(file("migrate"))
  .dependsOn(schema, parser)
  .settings(
    name := "sequala-migrate",
    parserModuleSettings,
    libraryDependencies ++= Seq(
      "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-mysql" % testcontainersVersion % Test,
      "org.postgresql" % "postgresql" % "42.7.4" % Test,
      "com.mysql" % "mysql-connector-j" % "9.1.0" % Test,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.scalatestplus" %% "scalacheck-1-18" % "3.2.19.0" % Test,
    ),
  )

lazy val migratePostgres =
  project
  .in(file("migrate-postgres"))
  .dependsOn(migrate, schemaPostgres, parserPostgres, migrate % "test->test")
  .settings(
    name := "sequala-migrate-postgres",
    parserModuleSettings,
    libraryDependencies ++= Seq(
      "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test,
      "org.postgresql" % "postgresql" % "42.7.4" % Test,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.scalatestplus" %% "scalacheck-1-18" % "3.2.19.0" % Test,
    ),
  )

lazy val migrateOracle =
  project
  .in(file("migrate-oracle"))
  .dependsOn(migrate, schemaOracle, parserOracle, migrate % "test->test")
  .settings(
    name := "sequala-migrate-oracle",
    parserModuleSettings,
    libraryDependencies ++= Seq(
      "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersVersion % Test,
      "com.dimafeng" %% "testcontainers-scala-oracle-xe" % testcontainersVersion % Test,
      "com.oracle.database.jdbc" % "ojdbc11" % "23.6.0.24.10" % Test,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.scalatestplus" %% "scalacheck-1-18" % "3.2.19.0" % Test,
    ),
  )
