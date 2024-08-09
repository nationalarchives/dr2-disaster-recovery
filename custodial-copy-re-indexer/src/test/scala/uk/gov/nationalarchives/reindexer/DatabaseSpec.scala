package uk.gov.nationalarchives.reindexer

import cats.effect.IO
import org.scalatest.flatspec.AnyFlatSpec
import uk.gov.nationalarchives.utils.TestUtils.*
import cats.effect.unsafe.implicits.global
import fs2.Chunk
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor3}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import uk.gov.nationalarchives.reindexer.Configuration.{CoUpdate, Config, IoUpdate}

import java.nio.file.{Files, Path}
import java.util.UUID

class DatabaseSpec extends AnyFlatSpec with BeforeAndAfterEach with TableDrivenPropertyChecks with ScalaCheckDrivenPropertyChecks:

  val databaseUtils = new DatabaseUtils("test-reindexer-database")
  import databaseUtils.*

  override def afterEach(): Unit = Files.delete(Path.of("test-reindexer-database"))

  override def beforeEach(): Unit = createTable()

  given Configuration = new Configuration:
    override def config: Configuration.Config = Config("test-reindexer-database", "", "")

  "getIds" should "return a deduplicated list of ids" in {
    val idOne = UUID.randomUUID
    val idTwo = UUID.randomUUID
    val fileOneA = createFile(id = idOne).unsafeRunSync()
    val fileOneB = createFile(id = idOne).unsafeRunSync()
    val fileTwoA = createFile(id = idTwo).unsafeRunSync()
    val fileTwoB = createFile(id = idTwo).unsafeRunSync()
    val res = Database[IO].getIds.compile.toList.unsafeRunSync()
    res.sorted should equal(List(idOne, idTwo).sorted)
  }

  "getIds" should "return an empty list if there are no entries in the database" in {
    val res = Database[IO].getIds.compile.toList.unsafeRunSync()
    res should equal(Nil)
  }

  val updateValues: TableFor3[List[Configuration.ReIndexUpdate], String, String] = Table(
    ("reIndexUpdate", "expectedIoValue", "expectedCoValue"),
    (List(IoUpdate(UUID.randomUUID, "ioValue")), "ioValue", "zref"),
    (List(CoUpdate(UUID.randomUUID, "coValue")), "zref", "coValue"),
    (List(IoUpdate(UUID.randomUUID, "ioValue"), CoUpdate(UUID.randomUUID, "coValue")), "ioValue", "coValue")
  )

  forAll(updateValues) { (updates, expectedIoValue, expectedCoValue) =>
    "write" should s"write $expectedIoValue to the IO and $expectedCoValue to the  CO" in {
      val id = updates.find(_.value == "ioValue").map(_.id).getOrElse(UUID.randomUUID)
      val fileId = updates.find(_.value == "coValue").map(_.id).getOrElse(UUID.randomUUID)
      val ioFile = createFile(id = id).unsafeRunSync()
      val coFile = createFile(fileId = fileId).unsafeRunSync()

      val res = Database[IO].write("zref")(Chunk.from(updates)).unsafeRunSync()

      val ioFileResult = readFiles(id).unsafeRunSync().head
      val coFileResult = readFiles(coFile.id).unsafeRunSync().head

      ioFileResult.zref.contains(expectedIoValue) should equal(true)
      coFileResult.zref.contains(expectedCoValue) should equal(true)
      res should equal(updates.length)
    }
  }
  case class SearchParameters(columnName: String, value: String)

  val searchParameters: Gen[SearchParameters] = for {
    length <- Gen.choose(1, 9)
    columnName <- Gen.listOfN(length, Gen.alphaChar)
    value <- Gen.alphaNumStr
  } yield SearchParameters(columnName.mkString, value)

  forAll(searchParameters) { searchParameters =>
    "write" should s"write ${searchParameters.value} to the ${searchParameters.columnName} for the IO" in {
      val columnName = searchParameters.columnName
      val value = searchParameters.value
      addColumn(columnName)
      val ioFile = createFile().unsafeRunSync()

      val res = Database[IO].write(columnName)(Chunk.from(List(IoUpdate(ioFile.id, value)))).unsafeRunSync()

      val columnValue = getColumn(ioFile.id, columnName)
      columnValue should equal(value)
    }
  }
