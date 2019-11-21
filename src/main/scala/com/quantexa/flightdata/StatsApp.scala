package com.quantexa.flightdata

import java.util.Calendar

import com.quantexa.flightdata.analytics.FlightStatistics
import com.quantexa.flightdata.io.CsvDatasetReader
import org.apache.spark.sql.{Encoders, SparkSession}

object StatsApp {
  final val AppName = "flight-data"

  def main(args: Array[String]): Unit = {
    val spark = {
      // Allow running from IDE
      if (sys.env.getOrElse("ENV", "") == "IDE") {
        SparkSession.builder
          .master("local[2]")
          .appName(AppName)
          .getOrCreate
      } else {
        SparkSession.builder
          .appName(AppName)
          .getOrCreate
      }
    }

    import spark.implicits._

    val flightDataCsv = args(0)
    val passengerCsv = args(1)

    val flightDataReader = new CsvDatasetReader[FlightData](
      spark,
      flightDataCsv,
      Some(Encoders.product[FlightData].schema)
    )
    val passengerReader = new CsvDatasetReader[Passenger](
      spark,
      passengerCsv,
      Some(Encoders.product[Passenger].schema)
    )

    // Persist/cache flight data as it's used multiple times
    val flightDataDataset = flightDataReader.getDataset.persist()
    val passengerDataset = passengerReader.getDataset

    val flightStatistics = new FlightStatistics(spark)

    val calendar = Calendar.getInstance()

    println("---- Total number of flights for each month in 2017 ----")
    readLine()
    flightStatistics
      .flightsPerMonth(flightDataDataset, 2017)
      .orderBy($"month".asc)
      .show()

    println("---- 100 most frequent flyers ----")
    readLine()
    flightStatistics
      .frequentFlyers(flightDataDataset, passengerDataset, 100)
      .show()

    println("---- Longest journey without visiting the UK ----")
    readLine()
    flightStatistics
      .longestRuns(flightDataDataset, "uk")
      .sort($"longestRun".desc)
      .show()

    println("---- Passengers who've been on more than 3 flights together ----")
    readLine()
    flightStatistics
      .sharedFlights(flightDataDataset, 3)
      .sort($"noFlights".desc)
      .show()
  }
}
