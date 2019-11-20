package com.quantexa.flightdata.analytics

import java.sql.Date

import com.github.dwickern.macros.NameOf._
import com.quantexa.flightdata.{FlightData, Passenger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * This class provides methods to calculate various statistics for flight data.
  */
class FlightStatistics(spark: SparkSession) {

  import spark.implicits._

  // These will be inlined by the compiler
  final val NoFlightsCol = nameOf[FrequentFlyer](_.noFlights)
  final val PassengerIdCol = nameOf[Passenger](_.passengerId)
  final val FlightIdCol = nameOf[FlightData](_.flightId)
  final val FromCol = nameOf[FlightData](_.from)
  final val ToCol = nameOf[FlightData](_.to)
  final val LongestRunCol = nameOf[PassengerStatistics](_.longestRun)
  final val PassengerId1Col = nameOf[PassengerPairStatistics](_.passengerId1)
  final val PassengerId2Col = nameOf[PassengerPairStatistics](_.passengerId2)

  /**
    * Calculates the number of flights in each calendar month.
    *
    * @param flightData Dataset of [[FlightData]]
    * @param year       The year to calculate results for
    * @return A [[Dataset]] of [[MonthStatistics]]
    */
  def flightsPerMonth(flightData: Dataset[FlightData], year: Int): Dataset[MonthStatistics] = {
    flightData
      .map(r => Flight(r.flightId, r.from, r.to, r.date))
      .dropDuplicates
      .filter(_.date.getYear == year)
      .groupByKey(_.date.getMonth)
      .count
      .map(t => MonthStatistics(t._1, t._2.toInt))
  }

  /**
    * Finds frequent flyers.
    *
    * @param flightData Dataset of [[FlightData]]
    * @param passengers Dataset of [[Passenger]]s
    * @param maxResults Maximum number of frequent flyers to return
    * @return A [[Dataset]] of [[FrequentFlyer]]s
    */
  def frequentFlyers(flightData: Dataset[FlightData],
                     passengers: Dataset[Passenger],
                     maxResults: Int = 100): Dataset[FrequentFlyer] = {
    flightData
      .groupByKey(_.passengerId)
      .count
      .join(passengers, $"value" === passengers(PassengerIdCol))
      .select(passengers.columns.map(col) ++ Seq($"count(1)".as(NoFlightsCol)): _*)
      .sort(col(NoFlightsCol).desc)
      .limit(maxResults)
      .as[FrequentFlyer]
  }

  /**
    * Calculates the greatest number of countries a passenger has been in without being in a certain country 
    * (reference country). A journey must start and end in the reference country to be included in the results.
    *
    * @param flightData Dataset of [[FlightData]]
    * @param refCountry The reference country
    * @return A [[Dataset]] of [[PassengerStatistics]]
    */
  def longestRuns(flightData: Dataset[FlightData],
                  refCountry: String): Dataset[PassengerStatistics] = {
    val window = Window.partitionBy($"passengerId").orderBy($"date")

    flightData
      .withColumn("seq", row_number().over(window))
      .filter(col(FromCol) === refCountry || col(ToCol) === refCountry)
      .withColumn("noVisited", $"seq" - coalesce(lead($"seq", -1).over(window), $"seq"))
      .groupBy(col(PassengerIdCol))
      .agg(max($"noVisited").as(LongestRunCol))
      .as[PassengerStatistics]
  }

  /**
    * Finds passengers who have been on multiple flights together.
    *
    * @param flightData Dataset of [[FlightData]]
    * @param minFlights Minimun number of shared flights to look for
    * @param from       Optional date from. If provided, `to` must also be provided
    * @param to         Optional date to. If provided, `from` must also be provided
    * @return A [[Dataset]] of [[PassengerPairStatistics]]
    */
  def sharedFlights(flightData: Dataset[FlightData],
                    minFlights: Int,
                    from: Option[Date] = None,
                    to: Option[Date] = None): Dataset[PassengerPairStatistics] = {
    require(from.nonEmpty && to.nonEmpty || from.isEmpty && to.isEmpty)

    val passengerId1 = col(s"fd1.$PassengerIdCol")
    val passengerId2 = col(s"fd2.$PassengerIdCol")

    flightData
      .filter(
        from.map(d =>
          (r: FlightData) => r.date.getTime >= d.getTime
        ).getOrElse((_: FlightData) => true)
      )
      .filter(
        to.map(d =>
          (r: FlightData) => r.date.getTime <= d.getTime
        ).getOrElse((_: FlightData) => true)
      )
      .as("fd1")
      .join(flightData.as("fd2"), FlightIdCol)
      .filter(passengerId1 < passengerId2)
      .groupBy(passengerId1, passengerId2)
      .count
      .select(passengerId1.as(PassengerId1Col), passengerId2.as(PassengerId2Col), $"count".as(NoFlightsCol))
      .as[PassengerPairStatistics]
      .filter(_.noFlights >= minFlights)
  }
}

case class Flight(flightId: Long,
                  from: String,
                  to: String,
                  date: Date)

case class MonthStatistics(month: Int,
                           noFlights: Long)

case class FrequentFlyer(passengerId: Long,
                         firstName: String,
                         lastName: String,
                         noFlights: Long)

case class PassengerStatistics(passengerId: Long,
                               longestRun: Int)

case class PassengerPairStatistics(passengerId1: Long,
                                   passengerId2: Long,
                                   noFlights: Long)
