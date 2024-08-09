package uk.gov.nationalarchives.utils

import doobie.util.{Get, Put}
import io.ocfl.api.model.{DigestAlgorithm, ObjectVersionId}
import io.ocfl.api.{OcflConfig, OcflRepository}
import io.ocfl.core.OcflRepositoryBuilder
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig
import io.ocfl.core.storage.{OcflStorage, OcflStorageBuilder}

import java.nio.file.Paths
import java.time.Instant
import java.util.UUID
object Utils:

  given Get[UUID] = Get[String].map(UUID.fromString)

  given Put[UUID] = Put[String].contramap(_.toString)

  given Get[Instant] = Get[Long].map(Instant.ofEpochMilli)

  given Put[Instant] = Put[Long].contramap(_.toEpochMilli)

  final case class OcflFile(
      versionNum: Long,
      id: UUID,
      name: Option[String],
      fileId: UUID,
      zref: Option[String],
      path: Option[String],
      fileName: Option[String],
      ingestDateTime: Option[Instant],
      sourceId: Option[String],
      citation: Option[String]
  )

  extension (uuid: UUID) def toHeadVersion: ObjectVersionId = ObjectVersionId.head(uuid.toString)

  def createOcflRepository(ocflRepoDir: String, ocflWorkDir: String): OcflRepository = {
    val repoDir = Paths.get(ocflRepoDir)
    val workDir = Paths.get(ocflWorkDir)
    val storage: OcflStorage = OcflStorageBuilder.builder().fileSystem(repoDir).build
    val ocflConfig: OcflConfig = new OcflConfig()
    ocflConfig.setDefaultDigestAlgorithm(DigestAlgorithm.fromOcflName("sha256"))
    new OcflRepositoryBuilder()
      .defaultLayoutConfig(new HashedNTupleLayoutConfig())
      .storage(storage)
      .ocflConfig(ocflConfig)
      .prettyPrintJson()
      .workDir(workDir)
      .build()
  }
