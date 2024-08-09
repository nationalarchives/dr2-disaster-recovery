package uk.gov.nationalarchives.custodialcopy

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.effect.unsafe.implicits.global
import io.ocfl.api.exception.{CorruptObjectException, NotFoundException}
import io.ocfl.api.io.FixityCheckInputStream
import io.ocfl.api.model.*
import io.ocfl.api.{OcflFileRetriever, OcflObjectUpdater, OcflOption, OcflRepository}
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentCaptor
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito.{doNothing, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.TableDrivenPropertyChecks
import uk.gov.nationalarchives.custodialcopy.CustodialCopyObject.FileObject
import uk.gov.nationalarchives.custodialcopy.Main.IdWithSourceAndDestPaths
import uk.gov.nationalarchives.dp.client.Entities.Entity

import java.io.{ByteArrayInputStream, InputStream}
import java.lang
import java.nio.file.{Path, Paths}
import java.util.UUID
import java.util.function.Consumer
import scala.jdk.CollectionConverters.*

class OcflServiceTest extends AnyFlatSpec with MockitoSugar with TableDrivenPropertyChecks {

  private val url = "url"
  private val name = "name"
  val checksum = "checksum"
  private val destinationPath = "destinationPath"
  private val entity = mock[Entity]
  val semaphore: Semaphore[IO] = Semaphore[IO](1).unsafeRunSync()

  val testOcflFileRetriever: OcflFileRetriever = new OcflFileRetriever:
    override def retrieveFile(): FixityCheckInputStream =
      new FixityCheckInputStream(new ByteArrayInputStream("".getBytes), DigestAlgorithm.fromOcflName("sha256"), "checksum")

    override def retrieveRange(startPosition: lang.Long, endPosition: lang.Long): InputStream = new ByteArrayInputStream("".getBytes)

  def mockGetObjectResponse(
      ocflRepository: OcflRepository,
      id: UUID,
      ocflChecksum: String,
      destinationPath: String
  ): Unit = {
    val fileDetails = new FileDetails()
    fileDetails.setFixity(Map(DigestAlgorithm.fromOcflName("sha256") -> ocflChecksum).asJava)
    fileDetails.setPath(destinationPath)
    val ocflObjectVersionFile = new OcflObjectVersionFile(fileDetails, testOcflFileRetriever)
    val versionDetails = new VersionDetails()
    val v1 = VersionNum.V1
    versionDetails.setObjectVersionId(ObjectVersionId.version(id.toString, v1))
    val objectVersion = new OcflObjectVersion(versionDetails, Map(destinationPath -> ocflObjectVersionFile).asJava)
    when(ocflRepository.getObject(any[ObjectVersionId])).thenReturn(objectVersion)
  }

  "getMissingAndChangedObjects" should "return 1 missing DR object and 0 changed DR objects if the repository doesn't contain the OCFL object" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    when(ocflRepository.getObject(any[ObjectVersionId])).thenThrow(new NotFoundException)

    val service = new OcflService(ocflRepository, semaphore)
    val fileObjectThatShouldBeMissing =
      FileObject(id, name, checksum, url, destinationPath, UUID.randomUUID)

    val missingAndChangedObjects =
      service.getMissingAndChangedObjects(List(fileObjectThatShouldBeMissing)).unsafeRunSync()
    val missingObject = missingAndChangedObjects.missingObjects.head

    missingAndChangedObjects.missingObjects.size should equal(1)
    missingObject should equal(fileObjectThatShouldBeMissing)

    missingAndChangedObjects.changedObjects.size should equal(0)
  }

  "getMissingAndChangedObjects" should "return 1 missing DR object and 0 changed DR objects if the repository contains the OCFL object " +
    "but doesn't contain the file" in {
      val id = UUID.randomUUID()
      val ocflRepository = mock[OcflRepository]
      mockGetObjectResponse(ocflRepository, id, checksum, destinationPath)

      val service = new OcflService(ocflRepository, semaphore)
      val fileObjectThatShouldBeMissing =
        FileObject(id, name, checksum, url, "nonExistingDestinationPath", UUID.randomUUID)

      val missingAndChangedObjects =
        service.getMissingAndChangedObjects(List(fileObjectThatShouldBeMissing)).unsafeRunSync()
      val missingObject = missingAndChangedObjects.missingObjects.head

      missingAndChangedObjects.missingObjects.size should equal(1)
      missingObject should equal(fileObjectThatShouldBeMissing)

      missingAndChangedObjects.changedObjects.size should equal(0)
    }

  "getMissingAndChangedObjects" should "return 0 missing DR objects and 0 changed DR objects if the repository contains the DR object " +
    "but the checksums match" in {
      val id = UUID.randomUUID()
      val ocflRepository = mock[OcflRepository]
      mockGetObjectResponse(ocflRepository, id, checksum, destinationPath)

      val service = new OcflService(ocflRepository, semaphore)

      val missingAndChangedObjects =
        service
          .getMissingAndChangedObjects(
            List(FileObject(id, name, checksum, url, destinationPath, UUID.randomUUID))
          )
          .unsafeRunSync()

      missingAndChangedObjects.missingObjects.size should equal(0)
      missingAndChangedObjects.changedObjects.size should equal(0)
    }

  "getMissingAndChangedObjects" should "return 0 missing DR objects and 1 changed DR objects if the repository contains the DR object " +
    "but the checksums don't match" in {
      val id = UUID.randomUUID()
      val ocflRepository = mock[OcflRepository]
      mockGetObjectResponse(ocflRepository, id, checksum, destinationPath)

      val service = new OcflService(ocflRepository, semaphore)
      val fileObjectThatShouldHaveChangedChecksum =
        FileObject(id, name, "anotherChecksum", url, destinationPath, UUID.randomUUID)

      val missingAndChangedObjects =
        service.getMissingAndChangedObjects(List(fileObjectThatShouldHaveChangedChecksum)).unsafeRunSync()

      missingAndChangedObjects.missingObjects.size should equal(0)
      missingAndChangedObjects.changedObjects.size should equal(1)
      val changedObject = missingAndChangedObjects.changedObjects.head
      changedObject should equal(fileObjectThatShouldHaveChangedChecksum)
    }

  "getMissingAndChangedObjects" should "throw an exception if 'ocflRepository.getObject' returns an unexpected Exception" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    when(ocflRepository.getObject(any[ObjectVersionId])).thenThrow(new RuntimeException("unexpected Exception"))

    val service = new OcflService(ocflRepository, semaphore)
    val fileObjectThatShouldHaveChangedChecksum =
      FileObject(id, name, checksum, url, destinationPath, UUID.randomUUID)

    val ex = intercept[Exception] {
      service.getMissingAndChangedObjects(List(fileObjectThatShouldHaveChangedChecksum)).unsafeRunSync()
    }

    ex.getMessage should equal(
      s"'getObject' returned an unexpected error 'java.lang.RuntimeException: unexpected Exception' when called with object id $id"
    )
  }

  "getMissingAndChangedObjects" should "purge the failed object if there is a CorruptedObjectException and rethrow the error" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    val objectIdCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    when(ocflRepository.getObject(any[ObjectVersionId])).thenThrow(new CorruptObjectException())
    doNothing().when(ocflRepository).purgeObject(objectIdCaptor.capture())

    val service = new OcflService(ocflRepository, semaphore)
    val fileObjectThatShouldHaveChangedChecksum =
      FileObject(id, name, checksum, url, destinationPath, UUID.randomUUID)

    val ex = intercept[Exception] {
      service.getMissingAndChangedObjects(List(fileObjectThatShouldHaveChangedChecksum)).unsafeRunSync()
    }

    ex.getMessage should equal(
      s"Object $id is corrupt. The object has been purged and the error will be rethrown so the process can try again"
    )
    objectIdCaptor.getValue should equal(id.toString)
  }

  "createObjects" should "create DR objects in the OCFL repository" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    val objectVersionCaptor: ArgumentCaptor[ObjectVersionId] = ArgumentCaptor.forClass(classOf[ObjectVersionId])
    val updater = mock[OcflObjectUpdater]
    val sourceNioFilePathToAdd: ArgumentCaptor[Path] = ArgumentCaptor.forClass(classOf[Path])
    val destinationPathToAdd: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    val optionToAdd: ArgumentCaptor[OcflOption] = ArgumentCaptor.forClass(classOf[OcflOption])

    when(ocflRepository.updateObject(objectVersionCaptor.capture, any[VersionInfo], any[Consumer[OcflObjectUpdater]]))
      .thenAnswer { invocation =>
        val consumer = invocation.getArgument[Consumer[OcflObjectUpdater]](2)
        consumer.accept(updater)
        ObjectVersionId.head(id.toString)
      }
    val service = new OcflService(ocflRepository, semaphore)

    service.createObjects(List(IdWithSourceAndDestPaths(id, Paths.get("test"), destinationPath))).unsafeRunSync()

    UUID.fromString(objectVersionCaptor.getValue.getObjectId) should equal(id)
    verify(updater, times(1)).addPath(
      sourceNioFilePathToAdd.capture,
      destinationPathToAdd.capture,
      optionToAdd.capture
    )
    sourceNioFilePathToAdd.getAllValues.asScala.toList should equal(List(Paths.get("test")))
    destinationPathToAdd.getAllValues.asScala.toList should equal(List(destinationPath))
    optionToAdd.getAllValues.asScala.toList should equal(List(OcflOption.OVERWRITE))
  }

  "getAllFilePathsOnAnObject" should "throw an exception if the object to be deleted does not exist" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    when(ocflRepository.getObject(any[ObjectVersionId])).thenThrow(new NotFoundException)

    val service = new OcflService(ocflRepository, semaphore)

    val err = intercept[Exception] {
      service.getAllFilePathsOnAnObject(id).unsafeRunSync()
    }

    err.getMessage should equal(s"Object id $id does not exist")
  }

  "getAllFilePathsOnAnObject" should "return a path if the repository contains the OCFL object" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    mockGetObjectResponse(ocflRepository, id, checksum, destinationPath)

    val service = new OcflService(ocflRepository, semaphore)

    val filePathsOnAnObject = service.getAllFilePathsOnAnObject(id).unsafeRunSync()
    filePathsOnAnObject.foreach(_ should equal(destinationPath))
  }

  "getAllFilePathsOnAnObject" should "throw an exception if 'ocflRepository.getObject' returns an unexpected Exception" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    when(ocflRepository.getObject(any[ObjectVersionId])).thenThrow(new RuntimeException("unexpected Exception"))

    val service = new OcflService(ocflRepository, semaphore)

    val ex = intercept[Exception] {
      service.getAllFilePathsOnAnObject(id).unsafeRunSync()
    }

    ex.getMessage should equal(
      s"'getObject' returned an unexpected error 'java.lang.RuntimeException: unexpected Exception' when called with object id $id"
    )
  }

  "getAllFilePathsOnAnObject" should "purge the failed object if there is a CorruptedObjectException and rethrow the error" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    val objectIdCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    when(ocflRepository.getObject(any[ObjectVersionId])).thenThrow(new CorruptObjectException())
    doNothing().when(ocflRepository).purgeObject(objectIdCaptor.capture())

    val service = new OcflService(ocflRepository, semaphore)

    val ex = intercept[Exception] {
      service.getAllFilePathsOnAnObject(id).unsafeRunSync()
    }

    ex.getMessage should equal(
      s"Object $id is corrupt. The object has been purged and the error will be rethrown so the process can try again"
    )
    objectIdCaptor.getValue should equal(id.toString)
  }

  "deleteObjects" should "make the call to delete CC objects in the OCFL repository" in {
    val id = UUID.randomUUID()
    val ocflRepository = mock[OcflRepository]
    val objectVersionCaptor: ArgumentCaptor[ObjectVersionId] = ArgumentCaptor.forClass(classOf[ObjectVersionId])
    val updater = mock[OcflObjectUpdater]
    val pathsToDelete: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])

    when(ocflRepository.updateObject(objectVersionCaptor.capture, any[VersionInfo], any[Consumer[OcflObjectUpdater]]))
      .thenAnswer { invocation =>
        val consumer = invocation.getArgument[Consumer[OcflObjectUpdater]](2)
        consumer.accept(updater)
        ObjectVersionId.head(id.toString)
      }
    val service = new OcflService(ocflRepository, semaphore)

    service.deleteObjects(id, List(destinationPath, "destinationPath2")).unsafeRunSync()

    UUID.fromString(objectVersionCaptor.getValue.getObjectId) should equal(id)
    verify(updater, times(2)).removeFile(pathsToDelete.capture)

    pathsToDelete.getAllValues.asScala.toList should equal(List(destinationPath, "destinationPath2"))
  }
}
