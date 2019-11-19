package com.quantexa.flightdata.analytics

import java.sql.Date

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.quantexa.flightdata.{FlightData, SparkSessionTestWrapper}
import org.scalatest.WordSpec

class FlightStatisticsTests extends WordSpec with SparkSessionTestWrapper with DatasetComparer {

  import spark.implicits._

  private val flightStatistics = new FlightStatistics(spark)

  "FlightStatistics" when {

    "calling flightsPerMonth" should {
      "return correct dataset" in {
        val flightDataDataset = Seq(
          FlightData(1, 1, "", "", new Date(2017, 1, 1)),
          FlightData(2, 1, "", "", new Date(2017, 1, 1)),
          FlightData(5, 2, "", "", new Date(2017, 2, 5)),
          FlightData(6, 3, "", "", new Date(2017, 3, 1)),
        ).toDS
        val expectedDataset = Seq(
          MonthStatistics(1, 1),
          MonthStatistics(2, 1),
          MonthStatistics(3, 1)
        ).toDS
        val actualDataset = flightStatistics.flightsPerMonth(flightDataDataset, 2017)

        assertSmallDatasetEquality(actualDataset, expectedDataset, ignoreNullable = true)
      }
      "return empty dataset" in {
        ???
      }
    }

    "calling frequentFlyers" should {
      "return correct dataset" in {
        ???
      }
      "return empty dataset" in {
        ???
      }
    }

    "calling longestRuns" should {
      "return correct dataset" in {
        ???
      }
      "return empty dataset" in {
        ???
      }
    }

    "calling sharedFlights" should {
      "return correct dataset" in {
        ???
      }
      "return empty dataset" in {
        ???
      }
    }

  }
}
