package uk.gov.nationalarchives.builder

import cats.effect.IO
import org.scalatest.flatspec.AnyFlatSpec
import uk.gov.nationalarchives.builder.Main.Config
import uk.gov.nationalarchives.utils.TestUtils.*
import cats.effect.unsafe.implicits.global
import io.ocfl.api.exception.NotFoundException
import org.scalatest.matchers.should.Matchers.*

import java.util.UUID

class OcflSpec extends AnyFlatSpec:

  "generateOcflObjects" should "return the correct values" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(id)
    given Configuration = new Configuration:
      override def config: Config = Config("test-database", "http://localhost:9001", repoDir, workDir)

    val files = Ocfl[IO].generateOcflObjects(id).unsafeRunSync()

    val firstFileOpt = files.find(_.fileId == coRef)
    val secondFileOpt = files.find(_.fileId == coRefTwo)

    firstFileOpt.isDefined should equal(true)
    secondFileOpt.isDefined should equal(true)

    val firstFile = firstFileOpt.get
    val secondFile = secondFileOpt.get

    firstFile.id should equal(id)
    firstFile.name.get should equal("Title")
    firstFile.fileId should equal(coRef)
    firstFile.zref.get should equal("Zref")
    firstFile.fileName.get should equal("Content Title")

    secondFile.id should equal(id)
    secondFile.name.get should equal("Title")
    secondFile.fileId should equal(coRefTwo)
    secondFile.zref.get should equal("Zref")
    secondFile.fileName.get should equal("Content Title2")
  }

  "generateOcflObjects" should "fail if the id doesn't exist in the repository" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(id, addFilesToRepo = false)

    given Configuration = new Configuration:
      override def config: Config = Config("test-database", "http://localhost:9001", repoDir, workDir)

    val err = intercept[NotFoundException] {
      Ocfl[IO].generateOcflObjects(id).unsafeRunSync()
    }

    err.getMessage should equal(s"Object ObjectId{objectId='$id', versionNum='null'} was not found.")
  }
