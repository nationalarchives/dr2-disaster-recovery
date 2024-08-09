package uk.gov.nationalarchives.reindexer

import cats.effect.Sync
import cats.syntax.all.*
import io.ocfl.api.OcflRepository
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.w3c.dom.Document
import uk.gov.nationalarchives.reindexer.Configuration.{FileType, ReIndexUpdate}
import uk.gov.nationalarchives.utils.Utils.*

import java.io.File
import java.util.UUID
import javax.xml.parsers.DocumentBuilderFactory
import javax.xml.xpath.XPathExpression
import scala.jdk.CollectionConverters.*

trait Ocfl[F[_]: Sync]:

  def readValue(ioId: UUID, fileType: FileType, xpath: XPathExpression)(using configuration: Configuration): F[List[ReIndexUpdate]]
object Ocfl:

  private def fileToXml[F[_]: Sync](path: String): F[Document] = Sync[F].blocking {
    val xmlFile = new File(path)
    val builderFactory = DocumentBuilderFactory.newInstance()
    val builder = builderFactory.newDocumentBuilder()
    builder.parse(xmlFile)
  }

  def apply[F[_]: Sync](using ev: Ocfl[F]): Ocfl[F] = ev

  given impl[F[_]: Sync]: Ocfl[F] = new Ocfl[F]:

    def repo(configuration: Configuration): OcflRepository = createOcflRepository(configuration.config.ocflRepoDir, configuration.config.ocflWorkDir)

    override def readValue(ioId: UUID, fileType: FileType, xpath: XPathExpression)(using configuration: Configuration): F[List[ReIndexUpdate]] =
      for {
        logger <- Slf4jLogger.create[F]
        ocflObject <- Sync[F].onError(Sync[F].blocking(repo(configuration).getObject(ioId.toHeadVersion)))(err => logger.error(err)(err.getMessage))
        objectVersionFiles <- Sync[F].blocking(ocflObject.getFiles.asScala.filter(_.getPath.endsWith(s"${fileType}_Metadata.xml")))
        xmlFiles <- objectVersionFiles.toList.traverse(objectVersionFile =>
          fileToXml(s"${configuration.config.ocflRepoDir}/${objectVersionFile.getStorageRelativePath}")
        )
      } yield xmlFiles.map(xmlFile => fileType.getReindexUpdate(xpath, xmlFile))
