package uk.gov.nationalarchives.utils

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.syntax.all.*
import doobie.implicits.*
import doobie.util.Put
import doobie.util.transactor.Transactor.Aux
import doobie.{Fragment, Get, Transactor}
import io.ocfl.api.model.{ObjectVersionId, VersionInfo}
import io.ocfl.api.{OcflObjectUpdater, OcflOption}
import uk.gov.nationalarchives.utils.Utils.{OcflFile, createOcflRepository, given}

import java.nio.file.Files
import java.time.Instant
import java.time.temporal.ChronoField
import java.util.UUID
import scala.jdk.FunctionConverters.*
import scala.xml.Elem

object TestUtils:

  class DatabaseUtils(databaseName: String):

    val xa: Aux[IO, Unit] = Transactor.fromDriverManager[IO](
      driver = "org.sqlite.JDBC",
      url = s"jdbc:sqlite:$databaseName",
      logHandler = None
    )

    def createReIndexerTable(): Int =
      sql"CREATE TABLE IF NOT EXISTS files(version int, id text, name text, fileId text, zref text, path text, fileName text, ingestDateTime numeric, sourceId text, citation text);".update.run
        .transact(xa)
        .unsafeRunSync()

    def createTable(): Int =
      sql"CREATE TABLE IF NOT EXISTS files(version int, id text, name text, fileId text, zref text, path text, fileName text, ingestDateTime numeric, sourceId text, citation text);".update.run
        .transact(xa)
        .unsafeRunSync()

    def addColumn(columnName: String): Unit =
      (fr"ALTER TABLE files ADD COLUMN" ++ Fragment.const(columnName)).update.run.transact(xa).unsafeRunSync()

    def getColumn(id: UUID, columnName: String): String =
      (fr"select" ++ Fragment.const(columnName) ++ fr"from files where id = $id").query[String].unique.transact(xa).unsafeRunSync()

    def createFile(fileId: UUID = UUID.randomUUID, zref: String = "zref", id: UUID = UUID.randomUUID): IO[OcflFile] = {
      sql"""INSERT INTO files (version, id, name, fileId, zref, path, fileName, ingestDateTime, sourceId, citation)
                   VALUES (1, ${id.toString}, 'name', ${fileId.toString}, $zref, 'path', 'fileName', '2024-07-03T11:39:15.372Z', 'sourceId', 'citation')""".update.run
        .transact(xa)
        .map(_ => ocflFile(id, fileId, zref))
    }

    def createFile(id: UUID, zref: Option[String], sourceId: Option[String], citation: Option[String], ingestDateTime: Option[Instant]): IO[OcflFile] = {
      val fileId = UUID.randomUUID
      sql"""INSERT INTO files (version, id, name, fileId, zref, path, fileName, ingestDateTime, sourceId, citation)
                     VALUES (1, $id, 'name', $fileId, $zref, 'path', 'fileName', $ingestDateTime, $sourceId, $citation)""".update.run
        .transact(xa)
        .map(_ => OcflFile(1, id, "name".some, fileId, zref, "path".some, "fileName".some, ingestDateTime, sourceId, citation))
    }

    def readFiles(id: UUID): IO[List[OcflFile]] = {
      sql"SELECT * FROM files where id = $id"
        .query[OcflFile]
        .to[List]
        .transact(xa)
    }

  def ocflFile(id: UUID, fileId: UUID, zref: String = "zref"): OcflFile =
    OcflFile(
      1,
      id,
      "name".some,
      fileId,
      zref.some,
      "path".some,
      "fileName".some,
      Instant.now().`with`(ChronoField.NANO_OF_SECOND, 0).some,
      "sourceId".some,
      "citation".some
    )

  extension (uuid: UUID) def toHeadVersion: ObjectVersionId = ObjectVersionId.head(uuid.toString)

  val coRef: UUID = UUID.randomUUID
  val coRefTwo: UUID = UUID.randomUUID
  val ioRef: UUID = UUID.randomUUID

  val completeIoMetadataContent: Elem = <XIP>
    <Identifier>
      <Type>BornDigitalRef</Type>
      <Value>Zref</Value>
    </Identifier>
    <Identifier>
      <Type>SourceID</Type>
      <Value>SourceID</Value>
    </Identifier>
    <Identifier>
      <Type>NeutralCitation</Type>
      <Value>Citation</Value>
    </Identifier>
    <InformationObject>
      <Title>Title</Title>
      <Ref>{ioRef}</Ref>
    </InformationObject>
    <Metadata>
      <Content>
        <Source>
          <IngestDateTime>2024-07-03T11:39:15.372Z</IngestDateTime>
        </Source>
      </Content>
    </Metadata>
  </XIP>
  val completeCoMetadataContentElements: List[Elem] = List(
    <Metadata>
      <ContentObject>
        <Title>Content Title</Title>
        <Ref>{coRef}</Ref>
      </ContentObject>
    </Metadata>,
    <Metadata>
      <ContentObject>
        <Title>Content Title2</Title>
        <Ref>{coRefTwo}</Ref>
      </ContentObject>
    </Metadata>
  )

  def initialiseRepo(
      id: UUID,
      ioMetadataContent: Elem = completeIoMetadataContent,
      coMetadataContent: List[Elem] = completeCoMetadataContentElements,
      addFilesToRepo: Boolean = true,
      metadataFileSuffix: String = "_Metadata"
  ): (String, String) = {
    val repoDir = Files.createTempDirectory("repo")
    val workDir = Files.createTempDirectory("work")
    val metadataFileDirectory = Files.createTempDirectory("metadata")
    val ioMetadataFile = Files.createFile(metadataFileDirectory.resolve("IO_Metadata.xml"))

    val contentFile = Files.createTempFile("content", "file")
    Files.write(contentFile, "test".getBytes)

    Files.write(ioMetadataFile, ioMetadataContent.toString.getBytes)

    if (addFilesToRepo)
      createOcflRepository(repoDir.toString, workDir.toString).updateObject(
        id.toHeadVersion,
        new VersionInfo(),
        { (updater: OcflObjectUpdater) =>
          updater.addPath(ioMetadataFile, s"IO$metadataFileSuffix.xml", OcflOption.OVERWRITE)
          coMetadataContent.zipWithIndex.map { (elem, idx) =>
            val coMetadataFile = Files.createFile(metadataFileDirectory.resolve(s"$metadataFileSuffix$idx.xml"))
            Files.write(coMetadataFile, elem.toString.getBytes)
            updater.addPath(coMetadataFile, s"subfolder$idx/CO$metadataFileSuffix.xml", OcflOption.OVERWRITE)
            updater.addPath(contentFile, s"subfolder$idx/original/g1/content.file", OcflOption.OVERWRITE)
          }
          ()
        }.asJava
      )
    (repoDir.toString, workDir.toString)
  }
