package com.quantexa.flightdata.analytics

import java.sql.Date

import com.github.mrpowers.spark.fast.tests.DatasetComparer
import com.quantexa.flightdata.{FlightData, Passenger, SparkSessionTestWrapper}
import org.scalatest.WordSpec

class FlightStatisticsTests extends WordSpec with SparkSessionTestWrapper with DatasetComparer {

  import spark.implicits._

  private val flightStatistics = new FlightStatistics(spark)

  "FlightStatistics" when {

    "calling flightsPerMonth" should {
      "return correct dataset" in {
        val flightDataDataset = Seq(
          FlightData(1, 1, "", "", new Date(2017 - 1900, 1, 1)),
          FlightData(2, 1, "", "", new Date(2017 - 1900, 1, 1)),
          FlightData(5, 2, "", "", new Date(2017 - 1900, 2, 5)),
          FlightData(6, 3, "", "", new Date(2017 - 1900, 3, 1)),
        ).toDS
        val expectedDataset = Seq(
          MonthStatistics(1, 1),
          MonthStatistics(2, 1),
          MonthStatistics(3, 1),
        ).toDS
        val actualDataset = flightStatistics.flightsPerMonth(flightDataDataset, 2017)

        assertSmallDatasetEquality(actualDataset, expectedDataset, ignoreNullable = true)
      }
      "return empty dataset" in {
        val flightDataDataset = Seq[FlightData]().toDS
        val expectedDataset = Seq[MonthStatistics]().toDS
        val actualDataset = flightStatistics.flightsPerMonth(flightDataDataset, 2017)

        assertSmallDatasetEquality(actualDataset, expectedDataset, ignoreNullable = true)
      }
    }

    "calling frequentFlyers" should {
      "return correct dataset" in {
        val flightDataDataset = Seq(
          FlightData(1, 1, "", "", new Date(2017 - 1900, 1, 1)),
          FlightData(3, 1, "", "", new Date(2017 - 1900, 2, 5)),
          FlightData(1, 2, "", "", new Date(2017 - 1900, 2, 5)),
          FlightData(6, 3, "", "", new Date(2017 - 1900, 3, 1)),
          FlightData(3, 2, "", "", new Date(2017 - 1900, 2, 5)),
          FlightData(1, 4, "", "", new Date(2017 - 1900, 1, 1)),
        ).toDS
        val passengerDataset = Seq(
          Passenger(3, "Honey", "Bunny"),
          Passenger(1, "Jules", "Winnfield"),
          Passenger(6, "Quentin", "Tarantino"),
        ).toDS
        val expectedDataset = Seq(
          FrequentFlyer(1, "Jules", "Winnfield", 3),
          FrequentFlyer(3, "Honey", "Bunny", 2)
        ).toDS

        val actualDataset = flightStatistics.frequentFlyers(
          flightDataDataset,
          passengerDataset,
          2
        )

        assertSmallDatasetEquality(actualDataset, expectedDataset,
          ignoreNullable = true, orderedComparison = false)
      }
      "return empty dataset" in {
        val flightDataDataset = Seq[FlightData]().toDS
        val passengerDataset = Seq[Passenger]().toDS
        val expectedDataset = Seq[FrequentFlyer]().toDS
        val actualDataset = flightStatistics.frequentFlyers(flightDataDataset, passengerDataset, 2)

        assertSmallDatasetEquality(actualDataset, expectedDataset, ignoreNullable = true)
      }
    }

    "calling longestRuns" should {
      "return correct dataset" in {
        val flightDataDataset = Seq(
          FlightData(1, 1, "uk", "aa", new Date(2017 - 1900, 1, 1)),
          FlightData(2, 1, "uk", "aa", new Date(2017 - 1900, 1, 1)),
          FlightData(3, 1, "uk", "aa", new Date(2017 - 1900, 1, 1)),
          FlightData(1, 2, "aa", "bb", new Date(2017 - 1900, 1, 5)),
          FlightData(3, 2, "aa", "bb", new Date(2017 - 1900, 1, 5)),
          FlightData(3, 3, "bb", "uk", new Date(2017 - 1900, 2, 1)),
          FlightData(3, 4, "uk", "cc", new Date(2017 - 1900, 2, 2)),
          FlightData(1, 5, "bb", "cc", new Date(2017 - 1900, 2, 4)),
          FlightData(1, 6, "cc", "uk", new Date(2017 - 1900, 2, 6)),
          FlightData(3, 6, "cc", "uk", new Date(2017 - 1900, 2, 6)),
          FlightData(1, 7, "uk", "dd", new Date(2017 - 1900, 3, 1)),
          FlightData(2, 8, "aa", "uk", new Date(2017 - 1900, 3, 1)),

        ).toDS
        val expectedDataset = Seq(
          PassengerStatistics(1, 3),
          PassengerStatistics(2, 1),
          PassengerStatistics(3, 2),
        ).toDS

        val actualDataset = flightStatistics.longestRuns(flightDataDataset, "uk")

        assertSmallDatasetEquality(actualDataset, expectedDataset,
          ignoreNullable = true, orderedComparison = false)
      }
      "return empty dataset" in {
        val flightDataDataset = Seq[FlightData]().toDS
        val expectedDataset = Seq[PassengerStatistics]().toDS
        val actualDataset = flightStatistics.longestRuns(flightDataDataset, "uk")

        assertSmallDatasetEquality(actualDataset, expectedDataset, ignoreNullable = true)
      }
    }

    "calling sharedFlights with no dates" should {
      "return correct dataset" in {
        val flightDataDataset = Seq(
          FlightData(1, 1, "uk", "aa", new Date(2017 - 1900, 1, 1)),
          FlightData(2, 1, "uk", "aa", new Date(2017 - 1900, 1, 1)),
          FlightData(3, 1, "uk", "aa", new Date(2017 - 1900, 1, 1)),
          FlightData(1, 2, "aa", "bb", new Date(2017 - 1900, 1, 5)),
          FlightData(3, 2, "aa", "bb", new Date(2017 - 1900, 1, 5)),
          FlightData(2, 3, "bb", "uk", new Date(2017 - 1900, 2, 1)),
          FlightData(3, 3, "bb", "uk", new Date(2017 - 1900, 2, 1)),
          FlightData(3, 4, "uk", "cc", new Date(2017 - 1900, 2, 2)),
          FlightData(1, 5, "bb", "cc", new Date(2017 - 1900, 2, 4)),
          FlightData(1, 6, "cc", "uk", new Date(2017 - 1900, 2, 6)),
          FlightData(3, 6, "cc", "uk", new Date(2017 - 1900, 2, 6)),
        ).toDS
        val minFlights = 2
        val expectedDataset = Seq(
          PassengerPairStatistics(1, 3, 3),
          PassengerPairStatistics(2, 3, 2),
        ).toDS

        val actualDataset = flightStatistics.sharedFlights(flightDataDataset, minFlights)

        assertSmallDatasetEquality(actualDataset, expectedDataset,
          ignoreNullable = true, orderedComparison = false)
      }
      "return empty dataset" in {
        val flightDataDataset = Seq[FlightData]().toDS
        val expectedDataset = Seq[PassengerPairStatistics]().toDS
        val actualDataset = flightStatistics.sharedFlights(flightDataDataset, 2)

        assertSmallDatasetEquality(actualDataset, expectedDataset, ignoreNullable = true)
      }
    }
    "calling sharedFlights with dates" should {
      "return correct dataset" in {
        val flightDataDataset = Seq(
          FlightData(1, 1, "uk", "aa", new Date(2017 - 1900, 1, 1)),
          FlightData(2, 1, "uk", "aa", new Date(2017 - 1900, 1, 1)),
          FlightData(3, 1, "uk", "aa", new Date(2017 - 1900, 1, 1)),
          FlightData(1, 2, "aa", "bb", new Date(2017 - 1900, 1, 5)),
          FlightData(3, 2, "aa", "bb", new Date(2017 - 1900, 1, 5)),
          FlightData(2, 3, "bb", "uk", new Date(2017 - 1900, 2, 1)),
          FlightData(3, 3, "bb", "uk", new Date(2017 - 1900, 2, 1)),
          FlightData(3, 4, "uk", "cc", new Date(2017 - 1900, 2, 2)),
          FlightData(1, 5, "bb", "cc", new Date(2017 - 1900, 2, 4)),
          FlightData(1, 6, "cc", "uk", new Date(2017 - 1900, 2, 6)),
          FlightData(3, 6, "cc", "uk", new Date(2017 - 1900, 2, 6)),
        ).toDS
        val minFlights = 1
        val dateFrom = new Date(2017 - 1900, 1, 2)
        val dateTo = new Date(2017 - 1900, 2, 2)
        val expectedDataset = Seq(
          PassengerPairStatistics(1, 3, 1),
          PassengerPairStatistics(2, 3, 1),
        ).toDS

        val actualDataset = flightStatistics.sharedFlights(flightDataDataset, minFlights, Some(dateFrom), Some(dateTo))

        assertSmallDatasetEquality(actualDataset, expectedDataset,
          ignoreNullable = true, orderedComparison = false)
      }
    }
  }
}
