import sbtrelease.ReleaseStateTransformations.*
import Dependencies.*
import uk.gov.nationalarchives.sbt.Log4j2MergePlugin.log4j2MergeStrategy

import java.io.FileWriter
import java.net.URI

ThisBuild / organization := "uk.gov.nationalarchives"
ThisBuild / scalaVersion := "3.4.1"

lazy val setLatestTagOutput = taskKey[Unit]("Sets a GitHub actions output for the latest tag")

setLatestTagOutput := {
  val fileWriter = new FileWriter(sys.env("GITHUB_OUTPUT"), true)
  fileWriter.write(s"latest-tag=${(ThisBuild / version).value}\n")
  fileWriter.close()
}

publishArtifact := false

lazy val releaseSettings = Seq(
  releaseProcess := Seq[ReleaseStep](
    inquireVersions,
    runClean,
    runTest,
    setReleaseVersion,
    releaseStepTask(setLatestTagOutput),
    commitReleaseVersion,
    tagRelease,
    setNextVersion,
    commitNextVersion,
    pushChanges
  ),
  version := (ThisBuild / version).value,
  organization := "uk.gov.nationalarchives",
  organizationName := "National Archives",
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/nationalarchives/dr2-disaster-recovery"),
      "git@github.com:nationalarchives/dr2-disaster-recovery.git"
    )
  ),
  developers := List(
    Developer(
      id = "tna-digital-archiving-jenkins",
      name = "TNA Digital Archiving",
      email = "digitalpreservation@nationalarchives.gov.uk",
      url = url("https://github.com/nationalarchives/dr2-disaster-recovery")
    )
  ),
  description := "A client to communicate with the Preservica API",
  licenses := List("MIT" -> URI.create("https://choosealicense.com/licenses/mit/").toURL),
  homepage := Some(url("https://github.com/nationalarchives/dr2-disaster-recovery"))
)

lazy val root = (project in file("."))
  .settings(releaseSettings)
  .settings(
    name := "dr2-disaster-recovery",
    libraryDependencies ++= Seq(
      log4jSlf4j,
      log4jCore,
      log4jTemplateJson,
      preservicaClient,
      sqsClient,
      fs2Core,
      ocfl,
      pureConfig,
      pureConfigCatsEffect,
      scalaTest % Test,
      mockito % Test
    ),
    scalacOptions += "-deprecation"
  )
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "projectInfo"
  )

(assembly / assemblyJarName) := "dr2-disaster-recovery.jar"

scalacOptions ++= Seq("-Wunused:imports", "-Werror")

(assembly / assemblyMergeStrategy) := {
  case PathList(ps @ _*) if ps.last == "Log4j2Plugins.dat" => log4j2MergeStrategy
  case PathList("META-INF", xs @ _*) =>
    xs map { _.toLowerCase } match {
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.discard
    }
  case manifest if manifest.contains("MANIFEST.MF") => MergeStrategy.discard
  case x                                            => MergeStrategy.last
}
