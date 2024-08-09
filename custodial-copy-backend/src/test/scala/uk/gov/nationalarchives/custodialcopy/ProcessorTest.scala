package uk.gov.nationalarchives.custodialcopy

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import fs2.io.file.Path
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{doReturn, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatestplus.mockito.MockitoSugar
import uk.gov.nationalarchives.DASQSClient.MessageResponse
import uk.gov.nationalarchives.custodialcopy.CustodialCopyObject.MetadataObject
import uk.gov.nationalarchives.custodialcopy.Main.IdWithSourceAndDestPaths
import uk.gov.nationalarchives.custodialcopy.Message.{IoReceivedSnsMessage, ReceivedSnsMessage, SendSnsMessage}
import uk.gov.nationalarchives.custodialcopy.OcflService.MissingAndChangedObjects
import uk.gov.nationalarchives.custodialcopy.Processor.ObjectStatus.{Created, Deleted, Updated}
import uk.gov.nationalarchives.custodialcopy.Processor.ObjectType.{Metadata, MetadataAndPotentialBitstreams}
import uk.gov.nationalarchives.dp.client.EntityClient.IoMetadata
import uk.gov.nationalarchives.dp.client.EntityClient.RepresentationType.*
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.custodialcopy.testUtils.ExternalServicesTestUtils.*

import java.util.UUID
import scala.xml.Elem

class ProcessorTest extends AnyFlatSpec with MockitoSugar {
  "process" should "delete the file paths under an IO, if an IO message has 'deleted' set to 'true'" in {
    val paths = List("destinationPathToDelete", "destinationPath2ToDelete")
    val utils = new ProcessorTestUtils(pathsOfObjectsUnderIo = paths)

    utils.processor.process(utils.duplicatesIoMessageResponse, true).unsafeRunSync()
    utils.verifyCallsAndArguments(
      repTypes = Nil,
      repIndexes = Nil,
      idsOfEntityToGetMetadataFrom = Nil,
      entityTypesToGetMetadataFrom = Nil,
      xmlRequestsToValidate = Nil,
      createdIdSourceAndDestinationPathAndId = Nil,
      drosToLookup = Nil,
      snsMessagesToSend = List(
        SendSnsMessage(InformationObject, utils.ioId, MetadataAndPotentialBitstreams, Deleted, "")
      ),
      destinationPathsToDelete = paths
    )
  }

  "process" should "throw an Exception if a CO message has 'deleted' set to 'true'" in {
    val utils = new ProcessorTestUtils()

    val ex = intercept[Exception] {
      utils.processor.process(utils.duplicatesCoMessageResponses, true).unsafeRunSync()
    }

    ex.getMessage should equal(s"A Content Object '${utils.coId}' has been deleted in Preservica")
    utils.verifyCallsAndArguments(
      repTypes = Nil,
      repIndexes = Nil,
      idsOfEntityToGetMetadataFrom = Nil,
      entityTypesToGetMetadataFrom = Nil,
      xmlRequestsToValidate = Nil,
      createdIdSourceAndDestinationPathAndId = Nil,
      drosToLookup = Nil,
      snsMessagesToSend = Nil,
      receiptHandles = Nil,
      destinationPathsToDelete = Nil
    )
  }

  "process" should "retrieve the metadata if an information object update is received" in {
    val utils = new ProcessorTestUtils()

    utils.processor.process(utils.duplicatesIoMessageResponse, false).unsafeRunSync()
    utils.verifyCallsAndArguments(
      repTypes = Nil,
      repIndexes = Nil,
      createdIdSourceAndDestinationPathAndId = List(Nil, Nil)
    )
  }

  "process" should "retrieve the bitstream info for a CO message" in {
    val utils = new ProcessorTestUtils()
    val id = utils.coId
    val parentRef = utils.ioId

    utils.processor.process(utils.duplicatesCoMessageResponses, false).unsafeRunSync()

    utils.verifyCallsAndArguments(
      1,
      1,
      1,
      idsOfEntityToGetMetadataFrom = List(id),
      entityTypesToGetMetadataFrom = List(ContentObject),
      xmlRequestsToValidate = List(utils.coXmlToValidate),
      createdIdSourceAndDestinationPathAndId = List(Nil, Nil),
      drosToLookup = List(
        List(
          s"$parentRef/Preservation_1/$id/original/g1/90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt",
          s"$parentRef/Preservation_1/$id/CO_Metadata.xml"
        )
      ),
      receiptHandles = List("receiptHandle2")
    )
  }

  "process" should "throw an Exception if a Content Object does not have a parent" in {
    val utils = new ProcessorTestUtils(parentRefExists = false)

    val ex = intercept[Exception] {
      utils.processor.process(utils.duplicatesCoMessageResponses, false).unsafeRunSync()
    }

    ex.getMessage should equal("Cannot get IO reference from CO")
    utils.verifyCallsAndArguments(
      1,
      repTypes = Nil,
      repIndexes = Nil,
      idsOfEntityToGetMetadataFrom = Nil,
      entityTypesToGetMetadataFrom = Nil,
      xmlRequestsToValidate = Nil,
      createdIdSourceAndDestinationPathAndId = Nil,
      drosToLookup = Nil,
      receiptHandles = Nil
    )
  }

  "process" should "throw an Exception if a Content Object belongs to more than 1 Representation type" in {
    val utils = new ProcessorTestUtils(urlsToRepresentations =
      Seq(
        "http://testurl/representations/Preservation/1",
        "http://testurl/representations/Preservation/2",
        "http://testurl/representations/Access/1"
      )
    )
    val id = utils.coId

    val ex = intercept[Exception] {
      utils.processor.process(utils.duplicatesCoMessageResponses, false).unsafeRunSync()
    }

    ex.getMessage should equal(
      s"$id belongs to more than 1 representation type: Preservation_1, Preservation_2, Access_1"
    )
    utils.verifyCallsAndArguments(
      1,
      1,
      3,
      repTypes = List(Preservation, Preservation, Access),
      repIndexes = List(1, 2, 1),
      idsOfEntityToGetMetadataFrom = Nil,
      entityTypesToGetMetadataFrom = Nil,
      xmlRequestsToValidate = Nil,
      drosToLookup = Nil,
      receiptHandles = Nil
    )
  }

  "process" should "generate the correct destination paths for IO metadata" in {
    val utils = new ProcessorTestUtils()
    val id = utils.coId
    val parentRef = utils.ioId

    utils.processor.process(utils.duplicatesIoMessageResponse, false).unsafeRunSync()
    utils.verifyCallsAndArguments(
      0,
      0,
      idsOfEntityToGetMetadataFrom = List(parentRef),
      entityTypesToGetMetadataFrom = List(InformationObject),
      xmlRequestsToValidate = List(utils.ioXmlToValidate),
      createdIdSourceAndDestinationPathAndId = List(Nil, Nil),
      repTypes = Nil,
      repIndexes = Nil,
      drosToLookup = List(
        List(s"$parentRef/IO_Metadata.xml")
      ),
      receiptHandles = List(1).map(n => s"receiptHandle$n")
    )
  }

  "process" should "generate the correct destination paths for CO metadata and CO bitstreams" in {
    val utils = new ProcessorTestUtils()
    val id = utils.coId
    val parentRef = utils.ioId

    utils.processor.process(utils.duplicatesCoMessageResponses, false).unsafeRunSync()
    utils.verifyCallsAndArguments(
      1,
      1,
      1,
      idsOfEntityToGetMetadataFrom = List(id),
      entityTypesToGetMetadataFrom = List(ContentObject),
      xmlRequestsToValidate = List(utils.coXmlToValidate),
      createdIdSourceAndDestinationPathAndId = List(Nil, Nil),
      drosToLookup = List(
        List(
          s"$parentRef/Preservation_1/$id/original/g1/90dfb573-7419-4e89-8558-6cfa29f8fb16.testExt",
          s"$parentRef/Preservation_1/$id/CO_Metadata.xml"
        )
      ),
      receiptHandles = List(2).map(n => s"receiptHandle$n")
    )
  }

  "process" should "not create nor update objects if no objects are missing or changed" in {
    val utils = new ProcessorTestUtils()

    utils.processor.process(utils.duplicatesIoMessageResponse, false).unsafeRunSync()
    utils.verifyCallsAndArguments(
      repTypes = Nil,
      repIndexes = Nil,
      createdIdSourceAndDestinationPathAndId = List(Nil, Nil)
    )
  }

  "process" should "update but not create if the object has changed" in {
    val utils = new ProcessorTestUtils()
    val id = utils.ioId

    when(utils.ocflService.getMissingAndChangedObjects(any[List[MetadataObject]]))
      .thenReturn(
        IO.pure(
          MissingAndChangedObjects(
            Nil,
            List(
              MetadataObject(
                id,
                Some("Preservation_1"),
                "changed",
                "checksum",
                utils.ioConsolidatedMetadata,
                "destinationPath",
                "SourceIDValue"
              )
            )
          )
        )
      )

    utils.processor.process(utils.duplicatesIoMessageResponse, false).unsafeRunSync()
    utils.verifyCallsAndArguments(
      repTypes = Nil,
      repIndexes = Nil,
      createdIdSourceAndDestinationPathAndId = List(
        List(), // 1st call to 'createObjects' with missingObjectsPaths arg
        List(
          IdWithSourceAndDestPaths(id, Path(s"$id/changed").toNioPath, "destinationPath")
        ) // 2nd call to 'createObjects' with changedObjectsPaths arg
      ),
      snsMessagesToSend = List(
        SendSnsMessage(InformationObject, id, Metadata, Updated, "SourceIDValue")
      )
    )
  }

  "process" should "create but not update if the object is missing" in {
    val utils = new ProcessorTestUtils()
    val id = utils.ioId

    when(utils.ocflService.getMissingAndChangedObjects(any[List[MetadataObject]]))
      .thenReturn(
        IO.pure(
          MissingAndChangedObjects(
            List(
              MetadataObject(
                id,
                Some("Preservation_1"),
                "missing",
                "checksum",
                utils.ioConsolidatedMetadata,
                "destinationPath",
                "SourceIDValue"
              )
            ),
            Nil
          )
        )
      )

    utils.processor.process(utils.duplicatesIoMessageResponse, false).unsafeRunSync()

    utils.verifyCallsAndArguments(
      repTypes = Nil,
      repIndexes = Nil,
      createdIdSourceAndDestinationPathAndId = List(List(IdWithSourceAndDestPaths(id, Path(s"$id/missing").toNioPath, "destinationPath")), List()),
      snsMessagesToSend = List(
        SendSnsMessage(InformationObject, id, Metadata, Created, "SourceIDValue")
      )
    )
  }

  "process" should "update a changed file" in {
    val utils = new ProcessorTestUtils()
    val changedFileId = utils.ioId

    when(utils.ocflService.getMissingAndChangedObjects(any[List[MetadataObject]]))
      .thenReturn(
        IO.pure(
          MissingAndChangedObjects(
            Nil,
            List(
              MetadataObject(
                changedFileId,
                Some("Preservation_1"),
                "changed",
                "checksum",
                utils.ioConsolidatedMetadata,
                "destinationPath2",
                "SourceIDValue"
              )
            )
          )
        )
      )

    val response: MessageResponse[ReceivedSnsMessage] = MessageResponse[ReceivedSnsMessage](
      "receiptHandle2",
      IoReceivedSnsMessage(changedFileId, false)
    )

    utils.processor.process(response, false).unsafeRunSync()

    utils.verifyCallsAndArguments(
      repTypes = Nil,
      repIndexes = Nil,
      idsOfEntityToGetMetadataFrom = List(changedFileId),
      xmlRequestsToValidate = List(utils.ioXmlToValidate),
      createdIdSourceAndDestinationPathAndId = List(
        Nil,
        List(IdWithSourceAndDestPaths(changedFileId, Path(s"$changedFileId/changed").toNioPath, "destinationPath2"))
      ),
      drosToLookup = List(
        List(
          s"$changedFileId/IO_Metadata.xml"
        )
      ),
      snsMessagesToSend = List(
        SendSnsMessage(InformationObject, changedFileId, Metadata, Updated, "SourceIDValue")
      ),
      receiptHandles = List("receiptHandle2")
    )
  }

  "process" should "update a missing file" in {
    val utils = new ProcessorTestUtils()
    val missingFileId = UUID.randomUUID()
    val changedFileId = utils.ioId

    when(utils.ocflService.getMissingAndChangedObjects(any[List[MetadataObject]]))
      .thenReturn(
        IO.pure(
          MissingAndChangedObjects(
            List(
              MetadataObject(
                missingFileId,
                Some("Preservation_1"),
                "missing",
                "checksum",
                utils.ioConsolidatedMetadata,
                "destinationPath",
                "SourceIDValue"
              )
            ),
            Nil
          )
        )
      )

    val response: MessageResponse[ReceivedSnsMessage] =
      MessageResponse[ReceivedSnsMessage](
        "receiptHandle1",
        IoReceivedSnsMessage(missingFileId, false)
      )
    utils.processor.process(response, false).unsafeRunSync()

    utils.verifyCallsAndArguments(
      repTypes = Nil,
      repIndexes = Nil,
      idsOfEntityToGetMetadataFrom = List(missingFileId),
      xmlRequestsToValidate = List(utils.ioXmlToValidate),
      createdIdSourceAndDestinationPathAndId = List(
        List(IdWithSourceAndDestPaths(missingFileId, Path(s"$missingFileId/missing").toNioPath, "destinationPath")),
        Nil
      ),
      drosToLookup = List(
        List(
          s"$missingFileId/IO_Metadata.xml"
        )
      ),
      snsMessagesToSend = List(
        SendSnsMessage(InformationObject, missingFileId, Metadata, Created, "SourceIDValue")
      ),
      receiptHandles = List("receiptHandle1")
    )
  }

  "process" should "throw an Exception if XML string passed to validator is invalid" in {
    val utils = new ProcessorTestUtils()

    doReturn(
      IO.pure(
        IoMetadata(
          <InformationObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><InvalidTag/></InformationObject>,
          Seq(
            <Representation><InformationObject/><Name/><Type/><ContentObjects/><RepresentationFormats/><RepresentationProperties/></Representation>
          ),
          Seq(<Identifier><ApiId/><Type>SourceID</Type><Value>SourceId</Value></Identifier>),
          Seq(<Links><Link/></Links>),
          Seq(<Metadata><Content/></Metadata>),
          Seq(
            <EventAction commandType="command_create"><Event type="Ingest"><Ref/><Date/><User/></Event><Date></Date><Entity>a9e1cae8-ea06-4157-8dd4-82d0525b031c</Entity></EventAction>
          )
        )
      )
    ).when(utils.entityClient).metadataForEntity(ArgumentMatchers.argThat(new EntityWithSpecificType("IO")))

    val res = utils.processor.process(utils.duplicatesIoMessageResponse, false).attempt.unsafeRunSync()

    res.left.foreach(
      _.getMessage should equal(
        """cvc-complex-type.2.4.a: Invalid content was found starting with element '{"http://preservica.com/XIP/v7.0":InvalidTag}'. """ +
          """One of '{"http://preservica.com/XIP/v7.0":Parent}' is expected."""
      )
    )

    utils.verifyCallsAndArguments(
      repTypes = Nil,
      repIndexes = Nil,
      createdIdSourceAndDestinationPathAndId = Nil,
      xmlRequestsToValidate = List(
        <XIP xmlns="http://preservica.com/XIP/v7.0">
          <InformationObject><Ref/><Title/><Description/><SecurityTag/><CustomType/><InvalidTag/></InformationObject>
          <Representation><InformationObject/><Name/><Type/><ContentObjects/><RepresentationFormats/><RepresentationProperties/></Representation>
          <Identifier><ApiId/><Type>SourceID</Type><Value>SourceId</Value></Identifier>
          <Links><Link/></Links>
          <Metadata><Content/></Metadata>
          <EventAction commandType="command_create"><Event type="Ingest"><Ref/><Date/><User/></Event><Date/><Entity>a9e1cae8-ea06-4157-8dd4-82d0525b031c</Entity></EventAction>
        </XIP>
      ),
      drosToLookup = Nil,
      receiptHandles = Nil
    )
  }

  "process" should "throw an Exception if an Unexpected Exception was returned from the OCFL service" in {
    val utils = new ProcessorTestUtils(throwErrorInMissingAndChangedObjects = true)

    val res = utils.processor.process(utils.duplicatesIoMessageResponse, false).attempt.unsafeRunSync()

    res.left.foreach(_.getMessage should equal("Unexpected Error"))

    utils.verifyCallsAndArguments(
      repTypes = Nil,
      repIndexes = Nil,
      receiptHandles = Nil
    )
  }
}
