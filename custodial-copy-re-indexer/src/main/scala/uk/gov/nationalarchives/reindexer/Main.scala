package uk.gov.nationalarchives.reindexer

import cats.effect.*
import com.monovore.decline.*
import com.monovore.decline.effect.*
import uk.gov.nationalarchives.reindexer.Arguments.*

object Main extends CommandIOApp(name = "cc-indexer", header = ""):

  override def main: Opts[IO[ExitCode]] = index
    .map(ReIndexer[IO].reIndex)
    .map(_ >> IO(ExitCode.Success))
