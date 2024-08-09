package uk.gov.nationalarchives.builder

import cats.effect.IO
import uk.gov.nationalarchives.builder.Main.Config
import uk.gov.nationalarchives.utils.TestUtils.*
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.{okJson, post, urlEqualTo}
import com.github.tomakehurst.wiremock.stubbing.StubMapping
import io.ocfl.api.model.DigestAlgorithm
import io.ocfl.core.util.DigestUtil
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import software.amazon.awssdk.http.async.SdkAsyncHttpClient
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsAsyncClient
import uk.gov.nationalarchives.DASQSClient
import cats.effect.unsafe.implicits.global
import org.scalatest.matchers.should.Matchers.*

import java.net.URI
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.xml.Elem

class MainSpec extends AnyFlatSpec with BeforeAndAfterEach with BeforeAndAfterAll {

  val databaseUtils = new DatabaseUtils("test-database")
  import databaseUtils.*

  override def beforeAll(): Unit = createTable()

  override def beforeEach(): Unit = sqsServer.resetAll()

  def body(id: UUID): String = {
    val md5 = DigestUtil.computeDigestHex(DigestAlgorithm.fromOcflName("md5"), s"{\"ioRef\":\"$id\"}")
    s"""{"Messages": [{"Body": "{\\"ioRef\\":\\"$id\\"}","MD5OfBody": "$md5","ReceiptHandle": "A"}]}"""
  }

  val sqsServer = new WireMockServer(9001)

  sqsServer.start()

  private lazy val httpClient: SdkAsyncHttpClient = NettyNioAsyncHttpClient.builder().build()
  private val sqsClient: SqsAsyncClient = SqsAsyncClient.builder
    .region(Region.EU_WEST_2)
    .endpointOverride(URI.create("http://localhost:9001"))
    .httpClient(httpClient)
    .build()
  val daSQSClient = new DASQSClient[IO](sqsClient)

  "runBuilder" should "write the correct items to the database for multiple content objects" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(id)
    initialiseSqs(id)

    given config: Configuration = new Configuration:
      override def config: Config = Config("test-database", "http://localhost:9001", repoDir, workDir)

    Main.runBuilder(daSQSClient).compile.drain.unsafeRunSync()

    val files = readFiles(id).unsafeRunSync()
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
    firstFile.ingestDateTime.get should equal(Instant.parse("2024-07-03T11:39:15.372Z"))
    firstFile.sourceId.get should equal("SourceID")
    firstFile.citation.get should equal("Citation")

    secondFile.id should equal(id)
    secondFile.name.get should equal("Title")
    secondFile.fileId should equal(coRefTwo)
    secondFile.zref.get should equal("Zref")
    secondFile.fileName.get should equal("Content Title2")
    secondFile.ingestDateTime.get should equal(Instant.parse("2024-07-03T11:39:15.372Z"))
    secondFile.sourceId.get should equal("SourceID")
    secondFile.citation.get should equal("Citation")
  }

  "runBuilder" should "write the correct items to the database for a single content object" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(id, coMetadataContent = completeCoMetadataContentElements.head :: Nil)
    initialiseSqs(id)

    given config: Configuration = new Configuration:
      override def config: Config = Config("test-database", "http://localhost:9001", repoDir, workDir)

    Main.runBuilder(daSQSClient).compile.drain.unsafeRunSync()

    val files = readFiles(id).unsafeRunSync()
    files.length should equal(1)
    val firstFile = files.head
    firstFile.id should equal(id)
    firstFile.name.get should equal("Title")
    firstFile.fileId should equal(coRef)
    firstFile.zref.get should equal("Zref")
    firstFile.fileName.get should equal("Content Title")
    firstFile.ingestDateTime.get should equal(Instant.parse("2024-07-03T11:39:15.372Z"))
    firstFile.sourceId.get should equal("SourceID")
    firstFile.citation.get should equal("Citation")
  }

  "runBuilder" should "copy the content file to the new location" in {
    val id = UUID.randomUUID
    val (repoDir, workDir) = initialiseRepo(id, coMetadataContent = completeCoMetadataContentElements.head :: Nil)
    initialiseSqs(id)

    given config: Configuration = new Configuration:
      override def config: Config = Config("test-database", "http://localhost:9001", repoDir, workDir)

    Main.runBuilder(daSQSClient).compile.drain.unsafeRunSync()

    val files = readFiles(id).unsafeRunSync()
    files.length should equal(1)
    val firstFile = files.head
    Files.readString(Paths.get(firstFile.path.get)) should equal("test")
  }

  "runBuilder" should "return empty string for missing metadata values" in {
    val id = UUID.randomUUID
    val coId = UUID.randomUUID
    val ioMetadata =
      <XIP>
        <Identifiers/>
        <InformationObject/>
        <Metadata>
          <Content>
            <Source>
              <IngestDateTime>2024-07-03T11:39:15.372Z</IngestDateTime>
            </Source>
          </Content>
        </Metadata>
      </XIP>

    val coMetadata =
      <Metadata>
        <ContentObject><Ref>{coId}</Ref></ContentObject>
      </Metadata>

    val (repoDir, workDir) = initialiseRepo(id, ioMetadata, coMetadata :: Nil)
    initialiseSqs(id)

    given config: Configuration = new Configuration:
      override def config: Config = Config("test-database", "http://localhost:9001", repoDir, workDir)

    Main.runBuilder(daSQSClient).compile.drain.unsafeRunSync()

    val files = readFiles(id).unsafeRunSync()

    files.size should equal(1)

    val file = files.head
    file.id should equal(id)
    file.name.isEmpty should equal(true)
    file.zref.isEmpty should equal(true)
    file.fileName.isEmpty should equal(true)
  }

  "runBuilder" should "call the correct SQS endpoints" in {
    val id = UUID.randomUUID
    initialiseSqs(id)
    val (repoDir, workDir) = initialiseRepo(id)

    given config: Configuration = new Configuration:
      override def config: Config = Config("test-database", "http://localhost:9001", repoDir, workDir)

    Main.runBuilder(daSQSClient).compile.drain.unsafeRunSync()

    val serveEvents = sqsServer.getAllServeEvents.asScala.toList

    serveEvents.last.getRequest.getBodyAsString should equal("""{"QueueUrl":"http://localhost:9001","MaxNumberOfMessages":10}""")
    serveEvents.head.getRequest.getBodyAsString should equal("""{"QueueUrl":"http://localhost:9001","ReceiptHandle":"A"}""")
  }

  def initialiseSqs(id: UUID): StubMapping = {
    val messageResponse = body(id)
    sqsServer.stubFor(post(urlEqualTo("/")).willReturn(okJson(messageResponse)))
  }

}
