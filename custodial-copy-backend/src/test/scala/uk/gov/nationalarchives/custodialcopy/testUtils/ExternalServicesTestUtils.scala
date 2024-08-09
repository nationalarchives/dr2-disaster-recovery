package uk.gov.nationalarchives.custodialcopy.testUtils

import cats.effect.IO
import cats.effect.std.Semaphore
import cats.effect.unsafe.implicits.global
import fs2.Stream
import io.circe.{Decoder, Encoder}
import io.ocfl.api.model.DigestAlgorithm
import io.ocfl.api.{OcflConfig, OcflRepository}
import io.ocfl.core.OcflRepositoryBuilder
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig
import io.ocfl.core.storage.OcflStorageBuilder
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doReturn, spy, times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.{ArgumentCaptor, ArgumentMatcher, ArgumentMatchers, Mockito}
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.{Assertion, EitherValues}
import org.scalatestplus.mockito.MockitoSugar
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse
import software.amazon.awssdk.services.sns.model.PublishBatchResponse
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.utils.TestUtils.*
import uk.gov.nationalarchives.custodialcopy.CustodialCopyObject.MetadataObject
import uk.gov.nationalarchives.custodialcopy.{CustodialCopyObject, Message, OcflService, Processor}
import uk.gov.nationalarchives.custodialcopy.Main.{Config, IdWithSourceAndDestPaths}
import uk.gov.nationalarchives.custodialcopy.Message.{CoReceivedSnsMessage, IoReceivedSnsMessage, ReceivedSnsMessage, SendSnsMessage, SoReceivedSnsMessage}
import uk.gov.nationalarchives.custodialcopy.OcflService.MissingAndChangedObjects
import uk.gov.nationalarchives.*
import uk.gov.nationalarchives.dp.client.Client.{BitStreamInfo, Fixity}
import uk.gov.nationalarchives.dp.client.Entities.{Entity, fromType}
import uk.gov.nationalarchives.dp.client.{EntityClient, ValidateXmlAgainstXsd}
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.EntityClient.GenerationType.*
import uk.gov.nationalarchives.dp.client.EntityClient.RepresentationType.*
import uk.gov.nationalarchives.dp.client.EntityClient.*
import uk.gov.nationalarchives.dp.client.ValidateXmlAgainstXsd.PreservicaSchema.XipXsdSchemaV7

import scala.jdk.FunctionConverters.*
import java.net.URI
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.{Failure, Success, Try}
import scala.xml.Utility.trim
import scala.xml.{Elem, NodeBuffer, Utility, XML}

object ExternalServicesTestUtils extends MockitoSugar with EitherValues {
  private val ioType = InformationObject.entityTypeShort
  private val coType = ContentObject.entityTypeShort
  private lazy val allRepresentationTypes: Map[String, RepresentationType] = Map(
    Access.toString -> Access,
    Preservation.toString -> Preservation
  )

  class EntityWithSpecificType(shortenedEntityType: String) extends ArgumentMatcher[Entity] {
    def matches(entity: Entity): Boolean = entity.entityType.exists(_.entityTypeShort == shortenedEntityType)

    override def toString: String = s"[entityTypeShort == $shortenedEntityType]"
  }

  private def mockPreservicaClient(
      ioId: UUID = UUID.fromString("049974f1-d3f0-4f51-8288-2a40051a663c"),
      coId: UUID = UUID.fromString("3393cd51-3c54-41a0-a9d4-5234a0ae47bf"),
      coId2: UUID = UUID.fromString("ed384a5c-689b-4f56-a47b-3690259a9998"),
      coId3: UUID = UUID.fromString("07747488-7976-4481-8fa4-b515c842d9a0"),
      metadataElems: Seq[Elem] = Nil,
      bitstreamInfo1: Seq[BitStreamInfo] = Nil,
      bitstreamInfo2: Seq[BitStreamInfo] = Nil,
      addAccessRepUrl: Boolean = false
  ) = {
    val preservicaClient = mock[EntityClient[IO, Fs2Streams[IO]]]
    val entityType = Some(ContentObject)
    val contentObjectResponse =
      Entity(entityType, coId, None, None, deleted = false, entityType.map(_.entityPath), parent = Option(ioId))
    val urlToRepresentations = Seq(
      s"http://localhost/api/entity/information-objects/$ioId/representations/Preservation/1"
    ) ++ (if addAccessRepUrl then Seq(s"http://localhost/api/entity/information-objects/$ioId/representations/Access/1")
          else Nil)
    Seq((coId, bitstreamInfo1), (coId2, bitstreamInfo2), (coId3, bitstreamInfo1)).foreach { case (id, bitstreamInfo) =>
      when(preservicaClient.getBitstreamInfo(ArgumentMatchers.eq(id))).thenReturn(IO(bitstreamInfo))
    }
    val combinedBitstreamInfoResponses = bitstreamInfo1 ++ bitstreamInfo2

    // For some reason, the regular "when" stubbing doesn't play nicely with "argThat"
    doReturn(
      IO.pure(
        IoMetadata(
          <InformationObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><Parent/></InformationObject>,
          Seq(
            <Representation><InformationObject/><Name/><Type/><ContentObjects><ContentObject/></ContentObjects><RepresentationFormats/><RepresentationProperties/></Representation>
          ),
          Seq(
            <Identifier><ApiId/><Type>SourceID</Type><Value>SourceIDValue</Value><Entity/></Identifier>,
            <Identifier><ApiId/><Type>sourceID</Type><Value>sourceIDValue</Value><Entity/></Identifier>
          ),
          Seq(<Link><Type/><FromEntity/><ToEntity/></Link>),
          metadataElems,
          Seq(
            <EventAction commandType="command_create"><Event type="Ingest"><Ref/><Date>2024-05-31T11:54:20.528Z</Date><User/></Event><Date>2024-05-31T11:54:20.528Z</Date><Entity>a9e1cae8-ea06-4157-8dd4-82d0525b031c</Entity><SerialisedCommand/></EventAction>
          )
        )
      )
    ).when(preservicaClient).metadataForEntity(ArgumentMatchers.argThat(new EntityWithSpecificType("IO")))

    doReturn(
      IO.pure(
        CoMetadata(
          <ContentObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><Parent/></ContentObject>,
          Seq(<Generation original="true" active="true"><ContentObject>someContent</ContentObject></Generation>),
          combinedBitstreamInfoResponses.map { bitstreamInfo =>
            <Bitstream><Filename>{bitstreamInfo.name}</Filename><FileSize>{
              bitstreamInfo.fileSize
            }</FileSize><Fixities><Fixity><FixityAlgorithmRef>{
              bitstreamInfo.fixity.algorithm
            }</FixityAlgorithmRef><FixityValue>{
              bitstreamInfo.fixity.value
            }</FixityValue></Fixity></Fixities></Bitstream>
          },
          Seq(<Identifier><ApiId/><Type/><Value/><Entity/></Identifier>),
          Seq(<Link><Type/><FromEntity/><ToEntity/></Link>),
          metadataElems,
          Seq(
            <EventAction commandType="command_create"><Event type="Ingest"><Ref/><Date>2024-05-31T11:54:20.528Z</Date><User/></Event><Date>2024-05-31T11:54:20.528Z</Date><Entity>a9e1cae8-ea06-4157-8dd4-82d0525b031c</Entity><SerialisedCommand/></EventAction>
          )
        )
      )
    ).when(preservicaClient).metadataForEntity(ArgumentMatchers.argThat(new EntityWithSpecificType("CO")))

    when(preservicaClient.getUrlsToIoRepresentations(ArgumentMatchers.eq(ioId), any[Option[RepresentationType]]))
      .thenReturn(IO(urlToRepresentations))
    when(
      preservicaClient.getContentObjectsFromRepresentation(
        ArgumentMatchers.eq(ioId),
        ArgumentMatchers.eq(Preservation),
        any[Int]
      )
    ).thenReturn(IO(Seq(contentObjectResponse, contentObjectResponse.copy(ref = coId2))))

    when(
      preservicaClient.getContentObjectsFromRepresentation(
        ArgumentMatchers.eq(ioId),
        ArgumentMatchers.eq(Access),
        any[Int]
      )
    ).thenReturn(IO(Seq(contentObjectResponse.copy(ref = coId3))))

    Seq(coId, coId2).foreach { id =>
      when(preservicaClient.getEntity(id, ContentObject))
        .thenReturn(IO(contentObjectResponse.copy(ref = id)))
    }
    combinedBitstreamInfoResponses.foreach { bitstreamInfo =>
      when(
        preservicaClient
          .streamBitstreamContent[Unit](any[Fs2Streams[IO]])(any[String], any())
      ).thenAnswer((invocation: InvocationOnMock) => {
        val stream = invocation.getArgument[Fs2Streams[IO]#BinaryStream => IO[Unit]](2)
        if Option(stream).isDefined then stream(Stream.emits(s"File content for ${bitstreamInfo.name}".getBytes)).unsafeRunSync()

        IO.unit
      })
    }

    preservicaClient
  }

  private def createExistingMetadataEntryInRepo(
      id: UUID,
      entityType: String,
      repo: OcflRepository,
      existingMetadata: Elem,
      destinationPath: String
  ) = {
    val xmlAsString = existingMetadata.toString()
    addFileToRepo(id, repo, xmlAsString, metadataFile(id, entityType), destinationPath)
  }

  private def addFileToRepo(
      id: UUID,
      repo: OcflRepository,
      bodyAsString: String,
      sourceFilePath: String,
      destinationPath: String
  ) = {
    val path = Files.createTempDirectory(id.toString)
    Files.createDirectories(Paths.get(path.toString, id.toString))
    val fullSourceFilePath = Paths.get(path.toString, sourceFilePath)
    Files.write(fullSourceFilePath, bodyAsString.getBytes)
    val semaphore: Semaphore[IO] = Semaphore[IO](1).unsafeRunSync()
    new OcflService(repo, semaphore)
      .createObjects(List(IdWithSourceAndDestPaths(id, fullSourceFilePath, destinationPath)))
      .unsafeRunSync()
  }

  private def createTestRepo(repoDir: Path = Files.createTempDirectory("repo")) = {
    val workDir = Files.createTempDirectory("work")
    new OcflRepositoryBuilder()
      .defaultLayoutConfig(new HashedNTupleLayoutConfig())
      .storage(((s: OcflStorageBuilder) => {
        s.fileSystem(repoDir)
        ()
      }).asJava)
      .ocflConfig(((config: OcflConfig) => {
        config.setDefaultDigestAlgorithm(DigestAlgorithm.fromOcflName("sha256"))
        ()
      }).asJava)
      .prettyPrintJson()
      .workDir(workDir)
      .build()
  }

  private def mockSqs(messages: List[ReceivedSnsMessage]): DASQSClient[IO] = {
    val sqsClient = mock[DASQSClient[IO]]
    val responses = IO {
      messages.zipWithIndex.map { case (message, idx) =>
        MessageResponse[ReceivedSnsMessage](s"handle$idx", message)
      }
    }
    when(sqsClient.receiveMessages[ReceivedSnsMessage](any[String], any[Int])(using any[Decoder[ReceivedSnsMessage]]))
      .thenReturn(responses)
    when(sqsClient.deleteMessage(any[String], any[String])).thenReturn(IO(DeleteMessageResponse.builder().build))
    sqsClient
  }

  private def mockSns(): DASNSClient[IO] = {
    val snsClient = mock[DASNSClient[IO]]
    val responses = List(PublishBatchResponse.builder().build())

    when(
      snsClient.publish[SendSnsMessage](any[String])(any[List[SendSnsMessage]])(using
        any[Encoder[SendSnsMessage]]
      )
    )
      .thenReturn(IO.pure(responses))
    snsClient
  }

  private def metadataFile(id: UUID, entityType: String, potentialPath: Option[String] = None) =
    s"$id/${potentialPath.map(_ + "/").getOrElse("")}${entityType}_Metadata.xml"

  class MainTestUtils(
      typesOfSqsMessages: List[EntityType] = List(InformationObject),
      objectVersion: Int = 1,
      typesOfMetadataFilesInRepo: List[EntityType] = Nil,
      fileContentToWriteToEachFileInRepo: List[String] = Nil,
      metadataElemsPreservicaResponse: Seq[Elem] = Seq(
        <Metadata><Ref/><Entity/><Content><thing xmlns="http://www.mockSchema.com/test/v42"></thing></Content></Metadata>
      ),
      bitstreamInfo1Responses: Seq[BitStreamInfo] = Seq(
        BitStreamInfo(
          "90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt",
          1,
          "",
          Fixity("SHA256", ""),
          1,
          Original,
          None,
          Some(UUID.randomUUID())
        )
      ),
      bitstreamInfo2Responses: Seq[BitStreamInfo] = Nil,
      addAccessRepUrl: Boolean = false
  ) {

    val config: Config = Config("", "", "", "", "", None, "", "")

    val bitstreamInfoResponsesWithSameName: Seq[BitStreamInfo] = bitstreamInfo1Responses.flatMap { bitstreamInfo1Response =>
      bitstreamInfo2Responses.filter { bitstreamInfo2Response =>
        bitstreamInfo1Response.name == bitstreamInfo2Response.name
      }
    }

    val coId1: UUID = UUID.randomUUID()
    val coId2: UUID = UUID.randomUUID()
    val coId3: UUID = if bitstreamInfoResponsesWithSameName.nonEmpty then coId1 else UUID.randomUUID()
    val ioId: UUID = bitstreamInfo1Responses.headOption.flatMap(_.parentRef).getOrElse(UUID.randomUUID())
    lazy val repoDir: Path = Files.createTempDirectory("repo")
    lazy val repo: OcflRepository = createTestRepo(repoDir)

    private val coIds: Seq[UUID] = List(coId1, coId2, coId3)
    private val sqsMessages: List[ReceivedSnsMessage] =
      typesOfSqsMessages.zipWithIndex.flatMap { case (entityType, index) =>
        entityType match { // create duplicates in order to test deduplication
          case InformationObject => (1 to 2).map(_ => IoReceivedSnsMessage(ioId))
          case ContentObject =>
            val coId = coIds(index)
            (1 to 2).map(_ => CoReceivedSnsMessage(coId))
          case StructuralObject => (1 to 2).map(_ => SoReceivedSnsMessage(UUID.randomUUID))
        }
      }
    val sqsClient: DASQSClient[IO] = mockSqs(sqsMessages)
    val snsClient: DASNSClient[IO] = mockSns()

    lazy val expectedIoMetadataFileDestinationPath: String = metadataFile(ioId, ioType)
    lazy val expectedCoMetadataFileDestinationPath: String = metadataFile(ioId, coType, Some(s"Preservation_1/$coId1"))
    lazy val expectedCoFileDestinationPath: String =
      s"$ioId/Preservation_1/$coId1/original/g1/${bitstreamInfo1Responses.head.name}"

    val preservicaClient: EntityClient[IO, Fs2Streams[IO]] = mockPreservicaClient(
      ioId,
      coId1,
      coId2,
      coId3,
      metadataElems = metadataElemsPreservicaResponse,
      bitstreamInfo1 = bitstreamInfo1Responses,
      bitstreamInfo2 = bitstreamInfo2Responses,
      addAccessRepUrl
    )

    private val ioMetadataInRepo =
      <XIP xmlns="http://preservica.com/XIP/v7.0">
          <InformationObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><Parent/></InformationObject>
          <Representation><InformationObject/><Name/><Type/><ContentObjects><ContentObject/></ContentObjects><RepresentationFormats/><RepresentationProperties/></Representation>
          <Identifier><ApiId/><Type>SourceID</Type><Value>SourceIDValue</Value><Entity/></Identifier>
          <Identifier><ApiId/><Type>sourceID</Type><Value>sourceIDValue</Value><Entity/></Identifier>
          <Link><Type/><FromEntity/><ToEntity/></Link>
          <Metadata><Ref/><Entity/><Content><thing xmlns="http://www.mockSchema.com/test/v42"></thing></Content></Metadata>
          <EventAction commandType="command_create"><Event type="Ingest"><Ref/><Date>2024-05-31T11:54:20.528Z</Date><User/></Event><Date>2024-05-31T11:54:20.528Z</Date><Entity>a9e1cae8-ea06-4157-8dd4-82d0525b031c</Entity><SerialisedCommand/></EventAction>
        </XIP>

    private val bitstreamNodesInRepo = (bitstreamInfo1Responses ++ bitstreamInfo2Responses).map { bitstreamInfo =>
      Seq(
        "\n          ",
        <Bitstream><Filename>{bitstreamInfo.name}</Filename><FileSize>{
          bitstreamInfo.fileSize
        }</FileSize><Fixities><Fixity><FixityAlgorithmRef>{
          bitstreamInfo.fixity.algorithm
        }</FixityAlgorithmRef><FixityValue>{bitstreamInfo.fixity.value}</FixityValue></Fixity></Fixities></Bitstream>
      )
    }
    private val coMetadataInRepo =
      <XIP xmlns="http://preservica.com/XIP/v7.0">
          <ContentObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><Parent/></ContentObject>
          <Generation original="true" active="true"><ContentObject>someContent</ContentObject></Generation>{
        bitstreamNodesInRepo
      }
          <Identifier><ApiId/><Type/><Value/><Entity/></Identifier>
          <Link><Type/><FromEntity/><ToEntity/></Link>
          <Metadata><Ref/><Entity/><Content><thing xmlns="http://www.mockSchema.com/test/v42"></thing></Content></Metadata>
          <EventAction commandType="command_create"><Event type="Ingest"><Ref/><Date>2024-05-31T11:54:20.528Z</Date><User/></Event><Date>2024-05-31T11:54:20.528Z</Date><Entity>a9e1cae8-ea06-4157-8dd4-82d0525b031c</Entity><SerialisedCommand/></EventAction>
        </XIP>

    typesOfMetadataFilesInRepo.foreach {
      case InformationObject =>
        createExistingMetadataEntryInRepo(
          ioId,
          ioType,
          repo,
          ioMetadataInRepo,
          expectedIoMetadataFileDestinationPath
        )
      case ContentObject =>
        createExistingMetadataEntryInRepo(
          ioId,
          coType,
          repo,
          coMetadataInRepo,
          expectedCoMetadataFileDestinationPath
        )
      case unexpectedEntityType => throw new Exception(s"Unexpected EntityType $unexpectedEntityType!")
    }

    private val bitstreamNames = (bitstreamInfo1Responses ++ bitstreamInfo2Responses).map(_.name)

    fileContentToWriteToEachFileInRepo.zip(bitstreamNames).foreach { case (data, name) =>
      addFileToRepo(ioId, repo, data, s"$ioId/$name", expectedCoFileDestinationPath)
    }
    val semaphore: Semaphore[IO] = Semaphore[IO](1).unsafeRunSync()
    val ocflService = new OcflService(repo, semaphore)
    val xmlValidator: ValidateXmlAgainstXsd[IO] = ValidateXmlAgainstXsd[IO](XipXsdSchemaV7)
    val processor = new Processor(config, sqsClient, ocflService, preservicaClient, xmlValidator, snsClient)

    def latestObjectVersion(repo: OcflRepository, id: UUID): Long =
      repo.getObject(id.toHeadVersion).getObjectVersionId.getVersionNum.getVersionNum

    private val potentialObjectVersion: Try[Long] = Try(latestObjectVersion(repo, ioId))

    potentialObjectVersion match {
      case Success(actualVersion)                              => actualVersion should equal(objectVersion)
      case Failure(_: io.ocfl.api.exception.NotFoundException) => objectVersion should equal(0)
      case _                                                   => throw new Exception("Unexpected result")
    }
  }

  class ProcessorTestUtils(
      genVersion: Int = 1,
      genType: GenerationType = Original,
      parentRefExists: Boolean = true,
      urlsToRepresentations: Seq[String] = Seq("http://testurl/representations/Preservation/1"),
      missingObjects: List[CustodialCopyObject] = Nil,
      changedObjects: List[CustodialCopyObject] = Nil,
      throwErrorInMissingAndChangedObjects: Boolean = false
  ) {
    val config: Config = Config("", "", "queueUrl", "", "", Option(URI.create("https://example.com")), "", "topicArn")
    val ioId: UUID = UUID.randomUUID()
    val coId: UUID = UUID.randomUUID()

    lazy val coMessage: CoReceivedSnsMessage = CoReceivedSnsMessage(coId)
    lazy val ioMessage: IoReceivedSnsMessage = IoReceivedSnsMessage(ioId)
    val sqsClient: DASQSClient[IO] = mock[DASQSClient[IO]]
    val entityClient: EntityClient[IO, Fs2Streams[IO]] = mock[EntityClient[IO, Fs2Streams[IO]]]
    val snsClient: DASNSClient[IO] = mock[DASNSClient[IO]]

    val duplicatesIoMessageResponse: MessageResponse[ReceivedSnsMessage] =
      MessageResponse[ReceivedSnsMessage]("receiptHandle1", ioMessage)

    val duplicatesCoMessageResponses: MessageResponse[ReceivedSnsMessage] =
      MessageResponse[ReceivedSnsMessage]("receiptHandle2", coMessage)

    private val potentialParentRef = if parentRefExists then Some(ioId) else None

    val bitstreamFromApi: BitStreamInfo = BitStreamInfo(
      "90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt",
      1,
      "url",
      Fixity("sha256", "checksum"),
      genVersion,
      genType,
      Some("CoTitle"),
      potentialParentRef
    )
    val bitstreamFromEndpoint: Seq[Elem] = Seq(
      <Bitstream><Filename>{bitstreamFromApi.name}</Filename> <FileSize>{
        bitstreamFromApi.fileSize
      }</FileSize><Fixities/></Bitstream>
    )
    lazy val ioConsolidatedMetadata: Elem =
      <XIP>
        {
        entityFromApi +: (representationFromApi ++ ioIdentifiersFromApi ++ linksFromApi ++ metadataFromApi ++ eventActionFromApi)
      }
      </XIP>
    lazy val coConsolidatedMetadata: Elem =
      <XIP>
        {
        entityFromApi +: (generationFromApi ++ bitstreamFromEndpoint ++ coIdentifiersFromApi ++ linksFromApi ++ metadataFromApi ++ eventActionFromApi)
      }
      </XIP>

    private val metadataFromApi =
      Seq(
        <Metadata><Ref/><Entity/><Content><thing xmlns="http://www.mockSchema.com/test/v42"></thing></Content></Metadata>
      )
    private val entityFromApi =
      <InformationObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><Parent/></InformationObject>
    private val ioIdentifiersFromApi = Seq(
      <Identifier><ApiId/><Type>SourceID</Type><Value>SourceIDValue</Value><Entity/></Identifier>,
      <Identifier><ApiId/><Type>sourceID</Type><Value>sourceIDValue</Value><Entity/></Identifier>
    )

    private val coIdentifiersFromApi = Seq(<Identifier><ApiId/><Type/><Value/><Entity/></Identifier>)
    private val representationFromApi =
      Seq(
        <Representation><InformationObject/><Name/><Type/><ContentObjects><ContentObject/></ContentObjects><RepresentationFormats/><RepresentationProperties/></Representation>
      )

    private val generationFromApi =
      Seq(<Generation original="true" active="true"><ContentObject/></Generation>)

    private val linksFromApi = Seq(<Link><Type/><FromEntity/><ToEntity/></Link>)
    private val eventActionFromApi =
      Seq(
        <EventAction commandType="command_create"><Event type="Ingest"><Ref/><Date>2024-05-31T11:54:20.528Z</Date><User/></Event><Date>2024-05-31T11:54:20.528Z</Date><Entity>a9e1cae8-ea06-4157-8dd4-82d0525b031c</Entity><SerialisedCommand/></EventAction>
      )

    private val getBitstreamsCoIdCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    private val metadataEntityCaptor: ArgumentCaptor[Entity] = ArgumentCaptor.forClass(classOf[Entity])
    private val identifiersEntityCaptor: ArgumentCaptor[Entity] = ArgumentCaptor.forClass(classOf[Entity])
    private val repTypeCaptor: ArgumentCaptor[RepresentationType] = ArgumentCaptor.forClass(classOf[RepresentationType])
    private val repIndexCaptor: ArgumentCaptor[Int] = ArgumentCaptor.forClass(classOf[Int])

    private val queueUrlCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    private val idWithSourceAndDestPathsCaptor: ArgumentCaptor[List[IdWithSourceAndDestPaths]] =
      ArgumentCaptor.forClass(classOf[List[IdWithSourceAndDestPaths]])
    private val metadataXmlStringToValidate: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    private val droLookupCaptor: ArgumentCaptor[List[CustodialCopyObject]] =
      ArgumentCaptor.forClass(classOf[List[CustodialCopyObject]])

    private val topicArnCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    private val messagesCaptor: ArgumentCaptor[List[SendSnsMessage]] =
      ArgumentCaptor.forClass(classOf[List[SendSnsMessage]])
    private val receiptHandleCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])

    private def mockOcflService(
        missingObjects: List[CustodialCopyObject] = Nil,
        changedObjects: List[CustodialCopyObject] = Nil,
        throwErrorInMissingAndChangedObjects: Boolean = false
    ): OcflService = {
      val ocflService = mock[OcflService]
      Mockito.reset(ocflService)
      if throwErrorInMissingAndChangedObjects then
        when(ocflService.getMissingAndChangedObjects(any[List[MetadataObject]]))
          .thenThrow(new RuntimeException("Unexpected Error"))
      else
        when(ocflService.getMissingAndChangedObjects(any[List[MetadataObject]]))
          .thenReturn(IO.pure(MissingAndChangedObjects(missingObjects, changedObjects)))
      when(ocflService.createObjects(any[List[IdWithSourceAndDestPaths]])).thenReturn(IO.unit)

      ocflService
    }

    when(entityClient.getBitstreamInfo(coId))
      .thenReturn(
        IO(
          Seq(bitstreamFromApi)
        )
      )
    potentialParentRef.foreach { parentRef =>
      when(entityClient.getUrlsToIoRepresentations(parentRef, None))
        .thenReturn(IO.pure(urlsToRepresentations))

      urlsToRepresentations.foreach { url =>
        val urlSplit = url.split("/").reverse
        val repType = allRepresentationTypes(urlSplit(1))
        val repIndex = urlSplit.head.toInt
        when(entityClient.getContentObjectsFromRepresentation(parentRef, repType, repIndex))
          .thenReturn(fromType[IO]("CO", coId, None, None, deleted = false, parent = Some(parentRef)).map(Seq(_)))
      }
    }

    doReturn(
      IO.pure(
        IoMetadata(
          entityFromApi,
          representationFromApi,
          ioIdentifiersFromApi,
          linksFromApi,
          metadataFromApi,
          eventActionFromApi
        )
      )
    ).when(entityClient).metadataForEntity(ArgumentMatchers.argThat(new EntityWithSpecificType("IO")))
    doReturn(
      IO.pure(
        CoMetadata(
          entityFromApi,
          generationFromApi,
          bitstreamFromEndpoint,
          coIdentifiersFromApi,
          linksFromApi,
          metadataFromApi,
          eventActionFromApi
        )
      )
    ).when(entityClient).metadataForEntity(ArgumentMatchers.argThat(new EntityWithSpecificType("CO")))

    when(
      snsClient.publish(ArgumentMatchers.eq("topicArn"))(ArgumentMatchers.any[List[SendSnsMessage]]())(using
        any[Encoder[SendSnsMessage]]
      )
    )
      .thenReturn(IO.pure(List(PublishBatchResponse.builder.build)))

    when(sqsClient.deleteMessage(ArgumentMatchers.eq("queueUrl"), ArgumentMatchers.startsWith("receiptHandle")))
      .thenReturn(IO.pure(DeleteMessageResponse.builder.build))

    val ocflService: OcflService = mockOcflService(missingObjects, changedObjects, throwErrorInMissingAndChangedObjects)
    val xmlValidator: ValidateXmlAgainstXsd[IO] = spy(ValidateXmlAgainstXsd[IO](XipXsdSchemaV7))

    val processor: Processor = new Processor(config, sqsClient, ocflService, entityClient, xmlValidator, snsClient)
    val ioXmlToValidate: Elem =
      <XIP xmlns="http://preservica.com/XIP/v7.0">
          <InformationObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><Parent/></InformationObject>
          <Representation><InformationObject/><Name/><Type/><ContentObjects><ContentObject/></ContentObjects><RepresentationFormats/><RepresentationProperties/></Representation>
          <Identifier><ApiId/><Type>SourceID</Type><Value>SourceIDValue</Value><Entity/></Identifier>
          <Identifier><ApiId/><Type>sourceID</Type><Value>sourceIDValue</Value><Entity/></Identifier>
          <Link><Type/><FromEntity/><ToEntity/></Link>
          <Metadata><Ref/><Entity/><Content><thing xmlns="http://www.mockSchema.com/test/v42"/></Content></Metadata>
          <EventAction commandType="command_create"><Event type="Ingest"><Ref/><Date>2024-05-31T11:54:20.528Z</Date><User/></Event><Date>2024-05-31T11:54:20.528Z</Date><Entity>a9e1cae8-ea06-4157-8dd4-82d0525b031c</Entity><SerialisedCommand/></EventAction>
        </XIP>
    val coXmlToValidate: Elem =
      <XIP xmlns="http://preservica.com/XIP/v7.0">
          <InformationObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><Parent/></InformationObject>
          <Generation original="true" active="true"><ContentObject/></Generation>
          <Bitstream><Filename>90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt</Filename><FileSize>1</FileSize><Fixities/></Bitstream>
          <Identifier><ApiId/><Type/><Value/><Entity/></Identifier>
          <Link><Type/><FromEntity/><ToEntity/></Link>
          <Metadata><Ref/><Entity/><Content><thing xmlns="http://www.mockSchema.com/test/v42"/></Content></Metadata>
          <EventAction commandType="command_create"><Event type="Ingest"><Ref/><Date>2024-05-31T11:54:20.528Z</Date><User/></Event><Date>2024-05-31T11:54:20.528Z</Date><Entity>a9e1cae8-ea06-4157-8dd4-82d0525b031c</Entity><SerialisedCommand/></EventAction>
        </XIP>

    def verifyCallsAndArguments(
        numOfGetBitstreamInfoCalls: Int = 0,
        numOfGetUrlsToIoRepresentationsCalls: Int = 0,
        numOfGetContentObjectsFromRepresentationCalls: Int = 0,
        repTypes: List[RepresentationType] = List(Preservation),
        repIndexes: List[Int] = List(1),
        idsOfEntityToGetMetadataFrom: List[UUID] = List(ioId),
        entityTypesToGetMetadataFrom: List[EntityType] = List(InformationObject),
        xmlRequestsToValidate: List[Elem] = List(ioXmlToValidate),
        createdIdSourceAndDestinationPathAndId: List[List[IdWithSourceAndDestPaths]] = Nil,
        drosToLookup: List[List[String]] = List(List(s"$ioId/IO_Metadata.xml")),
        snsMessagesToSend: List[SendSnsMessage] = Nil,
        receiptHandles: List[String] = List("receiptHandle1")
    ): Assertion = {

      verify(entityClient, times(numOfGetBitstreamInfoCalls)).getBitstreamInfo(getBitstreamsCoIdCaptor.capture)
      verify(entityClient, times(numOfGetUrlsToIoRepresentationsCalls))
        .getUrlsToIoRepresentations(ArgumentMatchers.eq(ioId), ArgumentMatchers.eq(None))
      verify(entityClient, times(numOfGetContentObjectsFromRepresentationCalls)).getContentObjectsFromRepresentation(
        ArgumentMatchers.eq(ioId),
        repTypeCaptor.capture(),
        repIndexCaptor.capture()
      )
      verify(entityClient, times(idsOfEntityToGetMetadataFrom.length)).metadataForEntity(metadataEntityCaptor.capture)

      verify(ocflService, times(drosToLookup.length)).getMissingAndChangedObjects(
        droLookupCaptor.capture()
      )
      verify(ocflService, times(createdIdSourceAndDestinationPathAndId.length))
        .createObjects(idWithSourceAndDestPathsCaptor.capture())
      verify(sqsClient, times(receiptHandles.length))
        .deleteMessage(queueUrlCaptor.capture, receiptHandleCaptor.capture)

      val numOfTimesSnsMsgShouldBeSent =
        if (snsMessagesToSend.nonEmpty || createdIdSourceAndDestinationPathAndId.flatten.nonEmpty) 1 else 0
      verify(snsClient, times(numOfTimesSnsMsgShouldBeSent))
        .publish(topicArnCaptor.capture)(messagesCaptor.capture())(using any[Encoder[SendSnsMessage]])

      verify(xmlValidator, times(xmlRequestsToValidate.length))
        .xmlStringIsValid(metadataXmlStringToValidate.capture())

      getBitstreamsCoIdCaptor.getAllValues.asScala.toList should equal(
        (1 to numOfGetBitstreamInfoCalls).toList.map(_ => coId)
      )

      metadataEntityCaptor.getAllValues.asScala.toList
        .lazyZip(idsOfEntityToGetMetadataFrom)
        .lazyZip(entityTypesToGetMetadataFrom)
        .foreach { case (capturedEntity, idOfEntity, entityTypeToGetMetadataFrom) =>
          capturedEntity.ref should equal(idOfEntity)
          capturedEntity.entityType.map(_ should equal(entityTypeToGetMetadataFrom))
        }

      repTypeCaptor.getAllValues.asScala.toList should equal(repTypes)
      repIndexCaptor.getAllValues.asScala.toList should equal(repIndexes)

      queueUrlCaptor.getAllValues.asScala.toList should equal(List.fill(receiptHandles.length)("queueUrl"))
      receiptHandleCaptor.getAllValues.asScala.toList should equal(receiptHandles)

      val expectedIdsWithSourceAndDestPath = createdIdSourceAndDestinationPathAndId.flatten
      idWithSourceAndDestPathsCaptor.getAllValues.asScala.toList.flatten.zipWithIndex.foreach { case (capturedIdWithSourceAndDestPath, index) =>
        val expectedIdWithSourceAndDestPath = expectedIdsWithSourceAndDestPath(index)
        capturedIdWithSourceAndDestPath.id should equal(expectedIdWithSourceAndDestPath.id)
        capturedIdWithSourceAndDestPath.sourceNioFilePath.toString
          .endsWith(expectedIdWithSourceAndDestPath.sourceNioFilePath.toString) should equal(true)
        capturedIdWithSourceAndDestPath.destinationPath should equal(expectedIdWithSourceAndDestPath.destinationPath)
      }
      val capturedLookupDestinationPaths = droLookupCaptor.getAllValues.asScala.toList.map(_.map(_.destinationFilePath))
      capturedLookupDestinationPaths should equal(drosToLookup)

      if (numOfTimesSnsMsgShouldBeSent > 0) {
        topicArnCaptor.getValue should equal("topicArn")
        messagesCaptor.getAllValues.asScala.toList should equal(List(snsMessagesToSend))
      } else ()

      metadataXmlStringToValidate.getAllValues.asScala.toList.map(s => trim(XML.loadString(s))) should equal(
        xmlRequestsToValidate.map(trim)
      )
    }
  }
}
