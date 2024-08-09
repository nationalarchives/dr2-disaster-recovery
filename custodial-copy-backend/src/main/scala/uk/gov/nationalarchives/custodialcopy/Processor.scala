package uk.gov.nationalarchives.custodialcopy

import cats.effect.IO
import cats.implicits.*
import fs2.Stream
import fs2.io.file.{Files, Flags}
import io.circe.Encoder
import org.apache.commons.codec.digest.DigestUtils
import org.typelevel.log4cats.slf4j.Slf4jLogger
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.{DASNSClient, DASQSClient}
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.custodialcopy.CustodialCopyObject.*
import uk.gov.nationalarchives.custodialcopy.Main.{Config, IdWithSourceAndDestPaths}
import uk.gov.nationalarchives.custodialcopy.Message.*
import uk.gov.nationalarchives.custodialcopy.Processor.ObjectStatus
import uk.gov.nationalarchives.custodialcopy.Processor.ObjectStatus.{Created, Deleted, Updated}
import uk.gov.nationalarchives.custodialcopy.Processor.ObjectType.{Bitstream, Metadata, MetadataAndPotentialBitstreams}
import uk.gov.nationalarchives.dp.client.{EntityClient, ValidateXmlAgainstXsd}
import uk.gov.nationalarchives.dp.client.EntityClient.RepresentationType.*
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.EntityClient.*
import uk.gov.nationalarchives.dp.client.Entities.{Entity, fromType}
import uk.gov.nationalarchives.dp.client.ValidateXmlAgainstXsd.PreservicaSchema.XipXsdSchemaV7

import java.util.UUID
import scala.xml.{Elem, Node}

class Processor(
    config: Config,
    sqsClient: DASQSClient[IO],
    ocflService: OcflService,
    entityClient: EntityClient[IO, Fs2Streams[IO]],
    xmlValidator: ValidateXmlAgainstXsd[IO],
    snsClient: DASNSClient[IO]
) {
  private val newlineAndIndent = "\n          "
  private def deleteMessage(receiptHandle: String): IO[DeleteMessageResponse] =
    sqsClient.deleteMessage(config.sqsQueueUrl, receiptHandle)

  private def createMetadataObject(
      ioRef: UUID,
      metadata: EntityMetadata,
      fileName: String,
      path: String,
      tableItemIdentifier: String | UUID,
      repType: Option[String] = None
  ): IO[List[MetadataObject]] =
    for {
      entitySpecificMetadata <-
        metadata match {
          case ioMetadata: IoMetadata =>
            IO(
              Seq(ioMetadata.entityNode)
                ++ ioMetadata.representations.flatMap(representation => Seq(newlineAndIndent, representation))
            )
          case coMetadata: CoMetadata =>
            IO(
              Seq(coMetadata.entityNode)
                ++ coMetadata.generationNodes.flatMap(generationNode => Seq(newlineAndIndent, generationNode))
                ++ coMetadata.bitstreamNodes.flatMap(bitstreamNode => Seq(newlineAndIndent, bitstreamNode))
            )
          case _ => IO.raiseError(new Exception(s"${metadata.getClass} is not a supported metadata type"))
        }

      consolidatedMetadata =
        entitySpecificMetadata
          ++ metadata.identifiers.flatMap(identifier => Seq(newlineAndIndent, identifier))
          ++ metadata.links.flatMap(link => Seq(newlineAndIndent, link))
          ++ metadata.metadataNodes.flatMap(metadataNode => Seq(newlineAndIndent, metadataNode))
          ++ metadata.eventActions.flatMap(eventAction => Seq(newlineAndIndent, eventAction))

      allMetadataAsXml =
        <XIP xmlns="http://preservica.com/XIP/v7.0">
          {consolidatedMetadata}
        </XIP>

      allMetadataAsXmlString = allMetadataAsXml.toString()

      _ <- xmlValidator.xmlStringIsValid(allMetadataAsXmlString)
    } yield List(
      MetadataObject(
        ioRef,
        repType,
        fileName,
        DigestUtils.sha256Hex(allMetadataAsXmlString),
        allMetadataAsXml,
        path,
        tableItemIdentifier
      )
    )

  private lazy val allRepresentationTypes: Map[String, RepresentationType] = Map(
    Access.toString -> Access,
    Preservation.toString -> Preservation
  )

  private def getRepresentationTypeOfCo(ioRef: UUID, urlOfRepresentation: String, coRef: UUID) = {
    val splitUrlReversed = urlOfRepresentation.split("/").reverse
    val index = splitUrlReversed.head.toInt
    val representationTypeAsString = splitUrlReversed(1)

    val representationType = allRepresentationTypes(representationTypeAsString)

    for {
      contentObjectsFromRep <- entityClient.getContentObjectsFromRepresentation(
        ioRef,
        representationType,
        index
      )
    } yield contentObjectsFromRep.collect {
      case contentObjectFromRep if contentObjectFromRep.ref == coRef => s"${representationType}_$index"
    }
  }

  private def createDestinationFilePath(
      ioRef: UUID,
      potentialRef: Option[UUID] = None,
      potentialRepTypeGroup: Option[String] = None,
      potentialGenType: Option[GenerationType] = None,
      potentialGenVersion: Option[Int] = None,
      fileName: String
  ): String =
    List(
      Some(ioRef),
      potentialRepTypeGroup,
      potentialRef,
      potentialGenType.map(_.toString.toLowerCase),
      potentialGenVersion.map(version => s"g$version"),
      Some(fileName)
    ).flatten.mkString("/")

  private def createMetadataFileName(entityTypeShort: String) = s"${entityTypeShort}_Metadata.xml"

  private def toCustodialCopyObject(receivedSnsMessage: ReceivedSnsMessage): IO[List[CustodialCopyObject]] = receivedSnsMessage match {
    case IoReceivedSnsMessage(ref, deleted) =>
      for {
        entity <- fromType[IO](InformationObject.entityTypeShort, ref, None, None, deleted = deleted)
        metadataFileName = createMetadataFileName(InformationObject.entityTypeShort)
        metadataObject <- entityClient.metadataForEntity(entity).flatMap { metadata =>
          val destinationFilePath = createDestinationFilePath(entity.ref, fileName = metadataFileName)
          createMetadataObject(
            entity.ref,
            metadata,
            metadataFileName,
            destinationFilePath,
            getSourceIdFromIdentifierNodes(metadata.identifiers)
          )
        }
      } yield metadataObject
    case CoReceivedSnsMessage(ref, deleted) =>
      for {
        bitstreamInfoPerCo <- entityClient.getBitstreamInfo(ref)
        entity <- fromType[IO](
          ContentObject.entityTypeShort,
          ref,
          None,
          None,
          deleted = deleted,
          parent = bitstreamInfoPerCo.headOption.flatMap(_.parentRef)
        )
        parentRef <- IO.fromOption(entity.parent)(new Exception("Cannot get IO reference from CO"))
        urlsOfRepresentations <- entityClient.getUrlsToIoRepresentations(parentRef, None)
        coRepTypes <- urlsOfRepresentations.map(getRepresentationTypeOfCo(parentRef, _, entity.ref)).flatSequence
        bitstreamIdentifiers = bitstreamInfoPerCo.map(_.name).map(removeFileExtension).toSet
        _ <- IO.raiseWhen(bitstreamIdentifiers.size != 1)(new Exception(s"Expected 1 bitstream identifiers, found ${bitstreamIdentifiers.size}"))
        _ <- IO.raiseWhen(coRepTypes.length > 1) {
          new Exception(s"${entity.ref} belongs to more than 1 representation type: ${coRepTypes.mkString(", ")}")
        }
        representationTypeGroup = coRepTypes.headOption
        metadataFileName = createMetadataFileName(ContentObject.entityTypeShort)
        metadata <- entityClient.metadataForEntity(entity).flatMap { metadataFragments =>
          val destinationFilePath = createDestinationFilePath(
            parentRef,
            Some(entity.ref),
            representationTypeGroup,
            fileName = metadataFileName
          )
          createMetadataObject(
            parentRef,
            metadataFragments,
            metadataFileName,
            destinationFilePath,
            bitstreamIdentifiers.head,
            representationTypeGroup
          )
        }
      } yield bitstreamInfoPerCo.toList.map { bitStreamInfo =>
        val destinationFilePath = createDestinationFilePath(
          parentRef,
          Some(entity.ref),
          representationTypeGroup,
          Some(bitStreamInfo.generationType),
          Some(bitStreamInfo.generationVersion),
          bitStreamInfo.name
        )

        FileObject(
          parentRef,
          bitStreamInfo.name,
          bitStreamInfo.fixity.value,
          bitStreamInfo.url,
          destinationFilePath,
          removeFileExtension(bitStreamInfo.name)
        )
      } ++ metadata
    case SoReceivedSnsMessage(_, _) => IO.pure(Nil)
  }

  private def download(custodialCopyObject: CustodialCopyObject) = custodialCopyObject match {
    case fo: FileObject =>
      for {
        writePath <- fo.sourceFilePath
        _ <- entityClient.streamBitstreamContent[Unit](Fs2Streams.apply)(
          fo.url,
          s => s.through(Files[IO].writeAll(writePath, Flags.Write)).compile.drain
        )
      } yield IdWithSourceAndDestPaths(fo.id, writePath.toNioPath, fo.destinationFilePath)
    case mo: MetadataObject =>
      val metadataXmlAsString = mo.metadata.toString
      for {
        writePath <- mo.sourceFilePath
        _ <- Stream
          .emit(metadataXmlAsString)
          .through(Files[IO].writeUtf8(writePath))
          .compile
          .drain
      } yield IdWithSourceAndDestPaths(mo.id, writePath.toNioPath, mo.destinationFilePath)
  }

  private def removeFileExtension(bitstreamName: String) = UUID.fromString {
    if (bitstreamName.contains(".")) bitstreamName.split('.').dropRight(1).mkString(".") else bitstreamName
  }

  private def getSourceIdFromIdentifierNodes(identifiers: Seq[Node]) = identifiers
    .collectFirst {
      case identifier if (identifier \ "Type").text == "SourceID" => (identifier \ "Value").text
    }
    .getOrElse("")

  private def generateSnsMessage(
      obj: CustodialCopyObject,
      status: ObjectStatus
  ): SendSnsMessage = {
    val objectType = obj match {
      case _: FileObject     => Bitstream
      case _: MetadataObject => Metadata
    }

    val entityType = obj.tableItemIdentifier match {
      case _: String => InformationObject
      case _: UUID   => ContentObject
    }

    SendSnsMessage(entityType, obj.id, objectType, status, obj.tableItemIdentifier)
  }

  given Encoder[SendSnsMessage] = (message: SendSnsMessage) => {
    Encoder
      .forProduct5("entityType", "ioRef", "objectType", "status", "tableItemIdentifier")(_ =>
        (message.entityType.toString, message.ioRef.toString, message.objectType.toString, message.status.toString, message.tableItemIdentifier.toString)
      )
      .apply(message)
  }

  private def processNonDeletedMessages(messageResponse: MessageResponse[ReceivedSnsMessage]): IO[List[SendSnsMessage]] =
    for {
      logger <- Slf4jLogger.create[IO]
      custodialCopyObjects <- toCustodialCopyObject(messageResponse.message)

      missingAndChangedObjects <- ocflService.getMissingAndChangedObjects(custodialCopyObjects)

      missingObjectsPaths <- missingAndChangedObjects.missingObjects.map(download).sequence
      changedObjectsPaths <- missingAndChangedObjects.changedObjects.map(download).sequence

      _ <- ocflService.createObjects(missingObjectsPaths)
      _ <- logger.info(s"${missingObjectsPaths.length} objects created")
      _ <- ocflService.createObjects(changedObjectsPaths)
      _ <- logger.info(s"${changedObjectsPaths.length} objects updated")

      createdObjsSnsMessages = missingAndChangedObjects.missingObjects.map(generateSnsMessage(_, Created))
      updatedObjsSnsMessages = missingAndChangedObjects.changedObjects.map(generateSnsMessage(_, Updated))

    } yield createdObjsSnsMessages ++ updatedObjsSnsMessages

  private def processDeletedEntities(messageResponse: MessageResponse[ReceivedSnsMessage]): IO[List[SendSnsMessage]] =
    messageResponse.message match {
      case IoReceivedSnsMessage(ref, _) =>
        for {
          filePaths <- ocflService.getAllFilePathsOnAnObject(ref)
          _ <- ocflService.deleteObjects(ref, filePaths)
          deletedObjsSnsMessages = List(SendSnsMessage(InformationObject, ref, MetadataAndPotentialBitstreams, Deleted, ""))
        } yield deletedObjsSnsMessages
      case CoReceivedSnsMessage(ref, _) =>
        IO.raiseError(new Exception(s"A Content Object '$ref' has been deleted in Preservica"))
      case _ =>
        IO.raiseError(new Exception(s"Entity type is not supported for deletion"))
    }

  def process(messageResponse: MessageResponse[ReceivedSnsMessage], entityDeleted: Boolean): IO[Unit] =
    for {
      logger <- Slf4jLogger.create[IO]
      entityTypeDeletionSupported =
        messageResponse.message match {
          case SoReceivedSnsMessage(_, _) => false
          case _                          => true
        }
      snsMessages <-
        if entityDeleted && entityTypeDeletionSupported then processDeletedEntities(messageResponse)
        else processNonDeletedMessages(messageResponse)
      _ <- IO.whenA(snsMessages.nonEmpty) {
        snsClient.publish[SendSnsMessage](config.topicArn)(snsMessages).map(_ => ())
      }
      _ <- logger.info(s"${snsMessages.length} 'created/updated objects' messages published to SNS")
      _ <- deleteMessage(messageResponse.receiptHandle)
    } yield ()
}
object Processor {
  def apply(
      config: Config,
      sqsClient: DASQSClient[IO],
      ocflService: OcflService,
      entityClient: EntityClient[IO, Fs2Streams[IO]],
      snsClient: DASNSClient[IO]
  ): IO[Processor] = IO(
    new Processor(config, sqsClient, ocflService, entityClient, ValidateXmlAgainstXsd[IO](XipXsdSchemaV7), snsClient)
  )

  enum ObjectStatus:
    case Created, Updated, Deleted

  enum ObjectType:
    case Bitstream, Metadata, MetadataAndPotentialBitstreams
}
