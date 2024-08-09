package uk.gov.nationalarchives.reindexer

import cats.effect.unsafe.implicits.global
import cats.effect.{ExitCode, IO}
import com.monovore.decline.effect.CommandIOApp
import com.monovore.decline.{Command, Opts}
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import uk.gov.nationalarchives.reindexer.Arguments.index

class MainSpec extends AnyFlatSpec with EitherValues:
  object MainTest extends CommandIOApp(name = "test-app", header = ""):
    override def main: Opts[IO[ExitCode]] = index.map(_ => IO(ExitCode.Success))

  def runInvalidMainTest(args: String*): List[String] =
    Command("", "")(MainTest.main)
      .parse(args)
      .left
      .value
      .errors

  "Main" should "return success if the arguments are valid" in {
    Command("", "")(MainTest.main)
      .parse(List("reindex", "--file-type", "IO", "--column-name", "test", "--xpath", "//Valid"))
      .value
      .unsafeRunSync()
      .code should equal(0)
  }

  "Main" should "return an error if the file-type is not IO or CO" in {
    val errors = runInvalidMainTest("reindex", "--file-type", "SO", "--column-name", "test", "--xpath", "//Valid")
    errors should equal(List("SO is not a valid value for file-type"))
  }

  "Main" should "return an error if the xpath is invalid" in {
    val errors = runInvalidMainTest("reindex", "--file-type", "IO", "--column-name", "test", "--xpath", "(")
    errors should equal(List("( is not a valid value for xpath"))
  }

  "Main" should "return an error if the column name is empty" in {
    val errors = runInvalidMainTest("reindex", "--file-type", "IO", "--column-name", "", "--xpath", "//Valid")
    errors should equal(List("The column name cannot be blank"))
  }

  "Main" should "return an error if the subcommand is missing" in {
    val errors = runInvalidMainTest("--file-type", "IO", "--column-name", "test", "--xpath", "//Valid")
    errors should equal(List("Unexpected option: --file-type"))
  }

  "Main" should "return an error if the file-type is missing" in {
    val errors = runInvalidMainTest("reindex", "--column-name", "test", "--xpath", "//Valid")
    errors should equal(List("Missing expected flag --file-type!"))
  }

  "Main" should "return an error if the column-name is missing" in {
    val errors = runInvalidMainTest("reindex", "--file-type", "IO", "--xpath", "//Valid")
    errors should equal(List("Missing expected flag --column-name!"))
  }

  "Main" should "return an error if the xpath is missing" in {
    val errors = runInvalidMainTest("reindex", "--file-type", "IO", "--column-name", "test")
    errors should equal(List("Missing expected flag --xpath!"))
  }
