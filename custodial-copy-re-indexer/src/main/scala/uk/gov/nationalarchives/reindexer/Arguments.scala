package uk.gov.nationalarchives.reindexer

import cats.data.Validated
import cats.implicits.*
import com.monovore.decline.*
import uk.gov.nationalarchives.reindexer.Configuration.FileType

import javax.xml.xpath.{XPathExpression, XPathFactory}
import scala.util.{Failure, Success, Try}

object Arguments:
  private def validatedArgument[T](argName: String)(fn: String => T): Argument[T] =
    Argument.from[T](argName) { string =>
      Try(fn(string)) match
        case Failure(_)     => Validated.invalidNel[String, T](s"$string is not a valid value for $argName")
        case Success(value) => Validated.validNel[String, T](value)
    }

  given Argument[FileType] = validatedArgument("file-type")(FileType.valueOf)

  given Argument[XPathExpression] = validatedArgument("xpath")(XPathFactory.newInstance.newXPath.compile)

  case class ReIndexArgs(fileType: FileType, columnName: String, xpath: XPathExpression)

  private val fileType: Opts[FileType] = Opts.option[FileType]("file-type", "Location of the field to be indexed. Possible values are IO or CO")
  private val columnName: Opts[String] =
    Opts.option[String]("column-name", "The name of the database column to write the value to").mapValidated { columnName =>
      if columnName.isEmpty then Validated.invalidNel("The column name cannot be blank") else Validated.validNel(columnName)
    }
  private val xPath: Opts[XPathExpression] = Opts.option[XPathExpression]("xpath", "The xpath to the value used to populate the database column")

  val index: Opts[ReIndexArgs] = Opts.subcommand("reindex", "Adds a new value from the metadata xml into the specified database column") {
    (fileType, columnName, xPath).mapN(ReIndexArgs.apply)
  }
