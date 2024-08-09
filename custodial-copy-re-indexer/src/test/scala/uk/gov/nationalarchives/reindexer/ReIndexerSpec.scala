package uk.gov.nationalarchives.reindexer

import cats.effect.unsafe.implicits.global
import cats.effect.IO
import fs2.Chunk
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.reindexer.Arguments.ReIndexArgs
import uk.gov.nationalarchives.reindexer.Configuration.{Config, FileType, IoUpdate}

import java.io.ByteArrayInputStream
import java.util.UUID
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.{XPathExpression, XPathFactory}

class ReIndexerSpec extends AnyFlatSpec with EitherValues:
  val uuids: List[UUID] = List(UUID.randomUUID, UUID.randomUUID, UUID.randomUUID)
  val xpath: XPathExpression = XPathFactory.newInstance.newXPath.compile("//B")
  val appConfig: Config = Config("", "", "")

  def expectedXPath(xPathExpression: XPathExpression, xpathToCheck: String): Boolean = {
    val factory = DocumentBuilderFactory.newInstance
    factory.setNamespaceAware(true)
    val doc = factory.newDocumentBuilder.parse(new ByteArrayInputStream("<A><B>test</B></A>".getBytes))
    xPathExpression.evaluate(doc) == XPathFactory.newInstance.newXPath.compile(xpathToCheck).evaluate(doc)
  }

  "reIndex" should "pass the correct values to the OCFL and Database methods" in {
    val appConfig = Config("", "", "")
    given Database[IO] = new Database[IO]:
      override def getIds: fs2.Stream[IO, UUID] = fs2.Stream.emits(uuids)

      override def write(columnName: String)(updates: Chunk[Configuration.ReIndexUpdate]): IO[Int] = {
        updates.toList.sortBy(_.id) should equal(uuids.sorted.map(uuid => IoUpdate(uuid, s"$uuid-IO")))
        IO(updates.size)
      }

    given Ocfl[IO] = new Ocfl[IO]:
      override def readValue(ioId: UUID, fileType: FileType, xpath: XPathExpression)(using
          configuration: Configuration
      ): IO[List[Configuration.ReIndexUpdate]] = {
        uuids.contains(ioId) should equal(true)
        fileType should equal(FileType.IO)
        expectedXPath(xpath, "//B") should be(true)
        IO(IoUpdate(ioId, s"$ioId-$fileType") :: Nil)
      }

    ReIndexer[IO].reIndex(ReIndexArgs(FileType.IO, "", xpath)).unsafeRunSync()
  }

  "reIndex" should "chunk the updates to the write method" in {
    val appConfig = Config("", "", "")

    given Database[IO] = new Database[IO]:
      override def getIds: fs2.Stream[IO, UUID] = fs2.Stream.emits(List.fill(1000)(UUID.randomUUID))

      override def write(columnName: String)(updates: Chunk[Configuration.ReIndexUpdate]): IO[Int] = {
        updates.toList.size should equal(100)
        IO(updates.size)
      }

    given Ocfl[IO] = new Ocfl[IO]:
      override def readValue(ioId: UUID, fileType: FileType, xpath: XPathExpression)(using
          configuration: Configuration
      ): IO[List[Configuration.ReIndexUpdate]] = {
        IO(IoUpdate(ioId, s"$ioId-$fileType") :: Nil)
      }

    ReIndexer[IO].reIndex(ReIndexArgs(FileType.IO, "", xpath)).unsafeRunSync()
  }

  "reIndex" should "not call the OCFL repo or the write method if there are no ids returned" in {
    given Database[IO] = new Database[IO]:
      override def getIds: fs2.Stream[IO, UUID] = fs2.Stream.emits(Nil)

      override def write(columnName: String)(updates: Chunk[Configuration.ReIndexUpdate]): IO[Int] = {
        false should be(true)
        IO(updates.size)
      }

    given Ocfl[IO] = new Ocfl[IO]:
      override def readValue(ioId: UUID, fileType: FileType, xpath: XPathExpression)(using
          configuration: Configuration
      ): IO[List[Configuration.ReIndexUpdate]] = {
        false should be(true)
        IO(IoUpdate(ioId, s"$ioId-$fileType") :: Nil)
      }

    ReIndexer[IO].reIndex(ReIndexArgs(FileType.IO, "", xpath)).unsafeRunSync()
  }

  "reIndex" should "raise an error if there is an error getting the IDs" in {
    given Database[IO] = new Database[IO]:
      override def getIds: fs2.Stream[IO, UUID] = fs2.Stream.raiseError(new Exception("Error getting IDs"))

      override def write(columnName: String)(updates: Chunk[Configuration.ReIndexUpdate]): IO[Int] = IO(1)

    given Ocfl[IO] = new Ocfl[IO]:
      override def readValue(ioId: UUID, fileType: FileType, xpath: XPathExpression)(using
          configuration: Configuration
      ): IO[List[Configuration.ReIndexUpdate]] =
        IO(IoUpdate(UUID.randomUUID, "") :: Nil)

    val err = ReIndexer[IO].reIndex(ReIndexArgs(FileType.IO, "", xpath)).attempt.map(_.left.value).unsafeRunSync().getMessage
    err should equal("Error getting IDs")
  }

  "reIndex" should "raise an error if there is an error reading from OCFL" in {
    given Database[IO] = new Database[IO]:
      override def getIds: fs2.Stream[IO, UUID] = fs2.Stream.emits(uuids)

      override def write(columnName: String)(updates: Chunk[Configuration.ReIndexUpdate]): IO[Int] = IO(1)

    given Ocfl[IO] = new Ocfl[IO]:
      override def readValue(ioId: UUID, fileType: FileType, xpath: XPathExpression)(using
          configuration: Configuration
      ): IO[List[Configuration.ReIndexUpdate]] =
        IO.raiseError(new Exception("Error reading from OCFL"))

    val err = ReIndexer[IO].reIndex(ReIndexArgs(FileType.IO, "", xpath)).attempt.map(_.left.value).unsafeRunSync().getMessage
    err should equal("Error reading from OCFL")
  }

  "reIndex" should "raise an error if there is an error writing to the database" in {
    given Database[IO] = new Database[IO]:
      override def getIds: fs2.Stream[IO, UUID] = fs2.Stream.emits(uuids)

      override def write(columnName: String)(updates: Chunk[Configuration.ReIndexUpdate]): IO[Int] =
        IO.raiseError(new Exception("Error writing to the database"))

    given Ocfl[IO] = new Ocfl[IO]:
      override def readValue(ioId: UUID, fileType: FileType, xpath: XPathExpression)(using
          configuration: Configuration
      ): IO[List[Configuration.ReIndexUpdate]] =
        IO(IoUpdate(UUID.randomUUID, "") :: Nil)

    val err = ReIndexer[IO].reIndex(ReIndexArgs(FileType.IO, "", xpath)).attempt.map(_.left.value).unsafeRunSync().getMessage
    err should equal("Error writing to the database")
  }
