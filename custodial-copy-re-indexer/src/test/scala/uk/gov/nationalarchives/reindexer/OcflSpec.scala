package uk.gov.nationalarchives.reindexer

import cats.effect.IO
import org.scalatest.flatspec.AnyFlatSpec
import uk.gov.nationalarchives.reindexer.Configuration.FileType
import uk.gov.nationalarchives.utils.TestUtils.*
import cats.effect.unsafe.implicits.global
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers.*

import java.nio.file.Files
import java.util.UUID
import javax.xml.xpath.{XPathExpression, XPathFactory}

class OcflSpec extends AnyFlatSpec with EitherValues:
  val ioXpath: XPathExpression = XPathFactory.newInstance.newXPath.compile("//Identifier[Type='BornDigitalRef']/Value")
  val coXpath: XPathExpression = XPathFactory.newInstance.newXPath.compile("//ContentObject/Title")

  private def getError(id: UUID, fileType: FileType)(using Configuration) =
    Ocfl[IO].readValue(id, fileType, ioXpath).attempt.map(_.left.value).unsafeRunSync().getMessage

  "readValue" should "read the correct IO value" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(id)
    given Configuration = new Configuration:
      override def config: Configuration.Config = Configuration.Config("test-database", repoDir, workDir)

    val res = Ocfl[IO].readValue(id, FileType.IO, ioXpath).unsafeRunSync().head

    res.id should equal(ioRef)
    res.value should equal("Zref")
  }

  "readValue" should "read the correct CO values" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(id)

    given Configuration = new Configuration:
      override def config: Configuration.Config = Configuration.Config("test-database", repoDir, workDir)

    val res = Ocfl[IO].readValue(id, FileType.CO, coXpath).unsafeRunSync()

    res.size should equal(2)

    val coOne = res.find(_.id == coRef).get
    val coTwo = res.find(_.id == coRefTwo).get

    coOne.value should equal("Content Title")
    coTwo.value should equal("Content Title2")
  }

  "readValue" should "raise an error of the id is not in the repository" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(id)
    val invalidId = UUID.randomUUID()
    val expectedErrorMessage = s"Object ObjectId{objectId='$invalidId', versionNum='null'} was not found."

    given Configuration = new Configuration:
      override def config: Configuration.Config = Configuration.Config("test-database", repoDir, workDir)

    val errorMessage = getError(invalidId, FileType.IO)

    errorMessage should equal(expectedErrorMessage)
  }

  "readValue" should "initialise a new repository if one doesn't exist" in {
    val id = UUID.randomUUID
    val workDir = Files.createTempDirectory("work").toString
    val repoDir = Files.createTempDirectory("repo").toString
    val expectedErrorMessage = s"Object ObjectId{objectId='$id', versionNum='null'} was not found."

    given Configuration = new Configuration:
      override def config: Configuration.Config = Configuration.Config("test-database", repoDir, workDir)

    val errorMessage = getError(id, FileType.IO)

    errorMessage should equal(expectedErrorMessage)
  }
