package uk.gov.nationalarchives.reindexer

import cats.effect.Async
import cats.implicits.*
import doobie.Update
import doobie.implicits.*
import doobie.util.log.LogHandler
import doobie.util.transactor.Transactor
import doobie.util.transactor.Transactor.Aux
import doobie.util.{Put, Write}
import fs2.{Chunk, Stream}
import uk.gov.nationalarchives.reindexer.Configuration.{CoUpdate, IoUpdate, ReIndexUpdate}
import uk.gov.nationalarchives.utils.Utils.given

import java.util.UUID

trait Database[F[_]]:
  def getIds: Stream[F, UUID]

  def write(columnName: String)(updates: Chunk[ReIndexUpdate]): F[Int]

object Database:
  def apply[F[_]](using db: Database[F]): Database[F] = db

  given impl[F[_]: Async](using configuration: Configuration): Database[F] = new Database[F] {

    val xa: Aux[F, Unit] = Transactor.fromDriverManager[F](
      driver = "org.sqlite.JDBC",
      url = s"jdbc:sqlite:${configuration.config.databasePath}",
      logHandler = Option(LogHandler.jdkLogHandler)
    )

    override def getIds: Stream[F, UUID] =
      sql"select id from files group by 1"
        .query[UUID]
        .stream
        .transact(xa)

    given Write[ReIndexUpdate] = Write[(String, UUID)].contramap(field => (field.value, field.id))

    override def write(columnName: String)(updates: Chunk[ReIndexUpdate]): F[Int] = {
      val (ioUpdates, coUpdates) = updates.toList.partition {
        case _: IoUpdate => true
        case _: CoUpdate => false
      }
      val ioSql = s"update files set $columnName = ? where id = ?"
      val coSql = s"update files set $columnName = ? where fileId = ?"
      val allTransactions = for {
        ioTransaction <- Update[ReIndexUpdate](ioSql).updateMany(ioUpdates)
        coTransaction <- Update[ReIndexUpdate](coSql).updateMany(coUpdates)
      } yield (ioTransaction, coTransaction)
      allTransactions.transact(xa).map { case (ioCount, coCount) =>
        ioCount + coCount
      }
    }
  }
