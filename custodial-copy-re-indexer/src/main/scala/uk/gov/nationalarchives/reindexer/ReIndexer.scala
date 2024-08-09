package uk.gov.nationalarchives.reindexer

import cats.effect.{ExitCode, IO, Ref, Sync}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import uk.gov.nationalarchives.reindexer.Configuration.{FileType, ReIndexUpdate}
import cats.implicits.*
import uk.gov.nationalarchives.reindexer.Arguments.ReIndexArgs
import fs2.Chunk
import javax.xml.xpath.XPathExpression

trait ReIndexer[F[_]: Sync]:
  def reIndex(reindexArgs: ReIndexArgs)(using Database[F], Ocfl[F]): F[Unit]

object ReIndexer:

  def apply[F[_]: Sync](using ev: ReIndexer[F]): ReIndexer[F] = ev

  given logger[F[_]: Sync]: Logger[F] = Slf4jLogger.getLogger[F]

  private def logCount[F[_]: Sync](ref: Ref[F, Int], updatedCount: Int): F[Unit] =
    ref
      .getAndUpdate(_ + updatedCount)
      .flatMap(existing => Logger[F].info(s"Written rows $existing to ${existing + updatedCount - 1} to the database"))

  given impl[F[_]: Sync: Ocfl: Database]: ReIndexer[F] = new ReIndexer[F]:
    override def reIndex(reindexArgs: ReIndexArgs)(using Database[F], Ocfl[F]): F[Unit] =
      val chunkSize = 100
      val ocfl = Ocfl[F]
      for {
        ref <- Ref[F].of(0)
        _ <- Database[F].getIds
          .evalMap[F, List[ReIndexUpdate]](id => {
            ocfl.readValue(id, reindexArgs.fileType, reindexArgs.xpath)
          })
          .chunkN(chunkSize)
          .map(_.flatMap(Chunk.from))
          .evalMap(Database[F].write(reindexArgs.columnName))
          .evalTap(logCount(ref, _))
          .compile
          .count
        total <- ref.get
        _ <- logger.info(s"Processed $total rows")
      } yield ()
