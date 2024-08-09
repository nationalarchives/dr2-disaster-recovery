package uk.gov.nationalarchives.custodialcopy

import cats.effect.IO
import cats.effect.std.Semaphore
import io.ocfl.api.exception.{CorruptObjectException, NotFoundException}
import io.ocfl.api.model.{DigestAlgorithm, VersionInfo}
import io.ocfl.api.{OcflConfig, OcflObjectUpdater, OcflOption, OcflRepository}
import io.ocfl.core.OcflRepositoryBuilder
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig
import io.ocfl.core.storage.OcflStorageBuilder
import uk.gov.nationalarchives.custodialcopy.Main.{Config, IdWithSourceAndDestPaths}
import uk.gov.nationalarchives.custodialcopy.OcflService.*
import uk.gov.nationalarchives.utils.Utils.*

import java.nio.file.Paths
import java.util.UUID
import scala.jdk.FunctionConverters.*

class OcflService(ocflRepository: OcflRepository, semaphore: Semaphore[IO]) {
  def createObjects(paths: List[IdWithSourceAndDestPaths]): IO[Unit] = semaphore.acquire >> IO.blocking {
    paths
      .groupBy(_.id)
      .view
      .toMap
      .map { case (id, paths) =>
        ocflRepository.updateObject(
          id.toHeadVersion,
          new VersionInfo(),
          { (updater: OcflObjectUpdater) =>
            paths.map { idWithSourceAndDestPath =>
              updater.addPath(
                idWithSourceAndDestPath.sourceNioFilePath,
                idWithSourceAndDestPath.destinationPath,
                OcflOption.OVERWRITE
              )
            }
            ()
          }.asJava
        )
      }
      .toList
  } >> semaphore.release

  def getMissingAndChangedObjects(
      objects: List[CustodialCopyObject]
  ): IO[MissingAndChangedObjects] = objects
    .foldLeft(IO(List[CustodialCopyObject](), List[CustodialCopyObject]())) { case (missingAndChangedIO, obj) =>
      val objectId = obj.id
      missingAndChangedIO.flatMap { missingAndChanged =>
        val (missingObjects, changedObjects) = missingAndChanged
        IO.blocking(ocflRepository.getObject(objectId.toHeadVersion))
          .map { ocflObject =>
            val potentialFile = Option(ocflObject.getFile(obj.destinationFilePath))
            potentialFile match {
              case Some(ocflFileObject) =>
                val checksumUnchanged =
                  Some(ocflFileObject.getFixity.get(DigestAlgorithm.fromOcflName("sha256"))).contains(obj.checksum)
                if checksumUnchanged then missingAndChanged else (missingObjects, obj :: changedObjects)
              case None =>
                (obj :: missingObjects, changedObjects) // Information Object exists but file doesn't
            }
          }
          .handleErrorWith {
            case _: NotFoundException => IO.pure(obj :: missingObjects, changedObjects)
            case coe: CorruptObjectException =>
              IO.blocking(ocflRepository.purgeObject(objectId.toString)) >>
                IO.raiseError(
                  new Exception(
                    s"Object $objectId is corrupt. The object has been purged and the error will be rethrown so the process can try again",
                    coe
                  )
                )
            case e: Throwable =>
              IO.raiseError(
                new Exception(
                  s"'getObject' returned an unexpected error '$e' when called with object id $objectId"
                )
              )
          }
      }
    }
    .map { missingAndChangedPair =>
      val (missingObjects, changedObjects) = missingAndChangedPair
      MissingAndChangedObjects(missingObjects, changedObjects)
    }
}
object OcflService {

  def apply(config: Config, semaphore: Semaphore[IO]): IO[OcflService] = IO {
    val repoDir = Paths.get(config.repoDir)
    val workDir =
      Paths.get(
        config.workDir
      )

    val repo: OcflRepository = new OcflRepositoryBuilder()
      .defaultLayoutConfig(new HashedNTupleLayoutConfig())
      .storage(((osb: OcflStorageBuilder) => {
        osb.fileSystem(repoDir)
        ()
      }).asJava)
      .ocflConfig(((config: OcflConfig) => {
        config.setDefaultDigestAlgorithm(DigestAlgorithm.fromOcflName("sha256"))
        ()
      }).asJava)
      .prettyPrintJson()
      .workDir(workDir)
      .build()
    new OcflService(repo, semaphore)
  }
  case class MissingAndChangedObjects(
      missingObjects: List[CustodialCopyObject],
      changedObjects: List[CustodialCopyObject]
  )
}
