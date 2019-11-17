package com.quantexa.flightdata.io

import java.io.FileWriter
import java.nio.file.Files

import com.quantexa.flightdata.SparkSessionTestWrapper
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.scalatest.WordSpec
import com.github.mrpowers.spark.fast.tests.DatasetComparer
import org.apache.spark.sql.Encoders


class CsvDatasetReaderTests extends WordSpec with SparkSessionTestWrapper with DatasetComparer {

  import spark.implicits._

  "A CsvDatasetReader" should {
    "return correct Dataset" in {
      val testCsvString =
        """
          |givenName,surname,age
          |Jules,Winnfield,40
          |Honey,Bunny,28
          |Quentin,Tarantino,56
        """.stripMargin
      val csvFile = Files.createTempFile("flightdata-test", "").toFile
      val csvWriter = new FileWriter(csvFile)
      try {
        csvWriter.write(testCsvString)
      } finally {
        csvWriter.close()
      }
      val schema = Encoders.product[Person].schema

      val expectedDataset = Seq(
        Person("Jules", "Winnfield", 40),
        Person("Honey", "Bunny", 28),
        Person("Quentin", "Tarantino", 56)
      ).toDS

      val csvDatasetReader = new CsvDatasetReader[Person](spark, csvFile.getAbsolutePath, Some(schema))
      val actualDataset = csvDatasetReader.getDataset

      assertSmallDatasetEquality(actualDataset, expectedDataset, ignoreNullable = true)
    }

    "return empty Dataset" in {
      val testCsvString = ""
      val csvFile = Files.createTempFile("flightdata-test", "").toFile
      val csvWriter = new FileWriter(csvFile)
      try {
        csvWriter.write(testCsvString)
      } finally {
        csvWriter.close()
      }
      val schema = new StructType()
        .add("givenName", DataTypes.StringType, false)
        .add("surname", DataTypes.StringType, false)
        .add("age", DataTypes.IntegerType, false)

      val expectedDataset = Seq[Person]().toDS

      val csvDatasetReader = new CsvDatasetReader[Person](spark, csvFile.getAbsolutePath, Some(schema))
      val actualDataset = csvDatasetReader.getDataset

      assertSmallDatasetEquality(actualDataset, expectedDataset, ignoreNullable = true)
    }

    "throw Exception for invalid CSV" in {
      ???
    }

    "throw Exception for incorrect schema" in {
      ???
    }
  }
}

case class Person(
                   givenName: String,
                   surname: String,
                   age: Int
                 )
