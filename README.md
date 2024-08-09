# DR2 Custodial Copy

This repository contains three components which together make up the custodial copy service.

## 1. Custodial Copy backend

This is a service which is intended to run in a long-running Docker container.

Every 20 seconds, it polls the queue specified in the `SQS_QUEUE_URL` environment variable.
This will be set to the queue which receives messages from the entity event generator.

### Messages

The queue sends messages in this format:

```json
{
  "id": "io:1b9555dd-43b7-4681-9b0d-85ebe951ca02",
  "deleted": Boolean
}
```

The prefix of the id can be `io`, `co` or `so` depending on the entity type; they are dealt with differently.
Messages are deduped before they are processed.
The "deleted" entry refers to the status on Preservica; the value could be `true` or `false`.

#### Handling Information Object (IO) messages, if entity has not been deleted

* Create the metadata file name `IO_Metadata.xml`
* Get the metadata from the Preservica API
* Create the destination path for this metadata file `{IO_REF}/IO_Metadata.xml`
* Wrap all returned metadata fragments in a `<AllMetadata/>` tag
* Calculate the checksum of this metadata string.
* Use all of this information to create a `MetadataObject`

#### Handling non-deleted Content Object (CO) messages, if entity has not been deleted

* Get the bitstream information (which contains the parent ID) from the Preservica API.
* Verify that the parent ID is present.
* Use parent ID to get the URLs of the representations
* Use URLs to get the COs under each representation
* Return the representation types (`{representationType}_{index}`) for a given CO ref
* If a CO has more than 1 representation type, throw an Exception
* Create the metadata file name `CO_Metadata.xml`
* Get the metadata from the Preservica API
* Create the destination path for this metadata file `{IO_REF}/{REP_TYPE}/{CO_REF}/{FILE_NAME}`
* Use metadata information to create a `MetadataObject`
* Create the destination path for the CO file `{IO_REF}/{REP_TYPE}/{CO_REF}/{GEN_TYPE}/g{GEN_VERSION}/{FILE_NAME}`
* Use the path and bitstream information to create a `FileObject`

#### Handling Information Object (IO) messages, if entity have been deleted
* Get all the paths to the files that sit underneath this object
* Delete all the paths
* Generate a `SendSnsMessage` object for it

#### Handling non-deleted Content Object (CO) messages, if entity have been deleted
We are not expected COs to be deleted in Preservica so if one is, an Exception will be thrown.

#### Handling Structural Object (SO) messages

Whether they are deleted or not, these are ignored as there is nothing in the structural objects we want to store.

### Example of the OCFL Structure
```
<IO_Ref>
├── IO_Metadata.xml
└── <Representation_Type>
    ├── <CO_Ref>
    │   ├── derived
    │   │   ├── g2
    │   │   │   └── 58154e7d-6271-488d-bf78-989d937580d5.pdf
    │   │   └── g3
    │   │       └── 58154e7d-6271-488d-bf78-989d937580d5.pdf
    │   ├── CO_Metadata.xml
    │   └── original
    │       └── g1
    │           └── 58154e7d-6271-488d-bf78-989d937580d5.docx
    └── <CO_Ref>
        ├── CO_Metadata.xml
        └── original
            └── g1
                └── c0c767b7-0eaf-41cc-b941-cabd60e50532.json
```

#### Looking up, Creating/Updating files and creating SNS messages

* Once the list of all `MetadataObject`s and `FileObject`s have been generated
* Check the OCFL repository for an object stored under this IO id.
    * If an object is not found that means no files belong under this IO and therefore, add the metadata object to the
      list of "missing" files
    * If an object is found:
        * Get the file, using the destination path
            * If the file is missing, add metadata object to the list of "missing" files
            * If the file is found
                * Compare the calculated checksum with the one in the OCFL repository.
                    * If they are the same, do nothing.
                    * If they are different, add the metadata object to the list of "changed" files
* Once the list of all "missing" and "changed" files are generated, stream them from Preservica if it is a bitstream 
  to a tmp directory or if it's a metadata update, convert the XML to a String and save it to a tmp directory 
    * For "missing" files:
        * Call `createObjects` on the OCFL repository in order to:
            * insert a new object into the `destinationPath` provided
            * add a new version to the OCFL repository
    * For "changed" files:
        * Call `createObjects` on the OCFL repository in order to:
            * overwrite the current file stored at the `destinationPath` provided
            * add a new version to the OCFL repository
* Finally, generate a list of SNS messages with information on this update; more information directly below

#### Sending Status messages to SNS
Once the process completes successfully, a message per OCFL update is sent to SNS with this information, the:
* entity type
* `ioRef`
* `ObjectStatus`: `Created`, `Updated` or `Deleted`
* `ObjectType`: `Bitstream`, `Metadata`, `MetadataAndPotentialBitstreams`
* `tableItemIdentifier` - the reference that can be used to find it in the files table

#### Deleting Received SQS messages

If the custodial copy process completes successfully, the messages that were received from SQS are then deleted from the SQS queue.
If any messages in a batch fail, all messages are left to try again. This avoids having to work out which ones were
successful which could be error-prone.

#### Parallel processing
Each message is processed in parallel, except for writing to the OCFL repository. 
The OCFL library will throw an exception if you try to write to the same object at the same time so there is a single semaphore to prevent two fibers writing at the same time.
All other processes such as fetching the data from Preservica and deleting the SQS messages are processed in parallel.
All non-deleted messages are processed (in parallel first) and then deleted ones messages to prevent unwanted behaviour 
like a deletion of an Information Object and then a CO being created afterward; this scenario could happen if a CO gets
added to Preservica (manually or automatically) and then the IO gets deleted within the time window.

### Infrastructure

This will be hosted on a machine at Kew rather than in the cloud so the only infrastructure resource needed is the
repository to store the docker image.

[Link to the infrastructure code](https://github.com/nationalarchives/dr2-terraform-environments)

### Environment Variables

| Name                   | Description                                                                 |
|------------------------|-----------------------------------------------------------------------------|
| PRESERVICA_URL         | The URL of the Preservica server                                            |
| PRESERVICA_SECRET_NAME | The Secrets Manager secret used to store the API credentials                |
| SQS_QUEUE_URL          | The queue the service will poll                                             |
| REPO_DIR               | The directory for the OCFL repository                                       |
| WORK_DIR               | The directory for the OCFL work directory                                   |
| HTTPS_PROXY            | An optional proxy. This is needed running in TNA's network but not locally. |

## 2. Frontend database builder

This is a service which listens to an SQS queue.
This queue receives a message whenever the main custodial-copy process adds or updates an object in the OCFL
repository.
Given the IO id, the builder service looks up the metadata from the metadata files in the OCFL repo and stores it in a
sqlite database.

### Environment Variables

| Name          | Description                                                                 |
|---------------|-----------------------------------------------------------------------------|
| QUEUE_URL     | The URL of the input queue                                                  |
| DATABASE_PATH | The path to the sqlite database                                             |
| SQS_QUEUE_URL | The queue the service will poll                                             |
| OCFL_REPO_DIR | The directory for the OCFL repository                                       |
| OCFL_WORK_DIR | The directory for the OCFL work directory                                   |
| HTTPS_PROXY   | An optional proxy. This is needed running in TNA's network but not locally. |

### Running locally.

You will need to create a sqlite3 database and run the following to create the files table:

```sql
CREATE TABLE files
(
    version  int,
    id       text,
    name     text,
    fileId   text,
    zref     text,
    path     text,
    fileName text
);
```

This can be run in Intellij by running the `uk.gov.nationalarchives.builder.Main` class and providing values for each of
the environment variables.

It can also be run using `sbt run`

## 3. Frontend

This is a webapp which allows a user to search for a file within the sqlite database.
If a file is found, the webapp allows the user to download that file by reading it directly from the OCFL repo

### Environment Variables

| Name          | Description                     |
|---------------|---------------------------------|
| DATABASE_PATH | The path to the sqlite database |

### Running locally.

The sqlite database must exist along with the file table.

This can be run in Intellij by running the `uk.gov.nationalarchives.webapp.Main` class and providing values for the
database path environment variable.

It can also be run using `sbt run`