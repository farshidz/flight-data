package com.quantexa.flightdata

import java.sql.Date

case class FlightData(passengerId: Int,
                      flightId: Int,
                      from: String,
                      to: String,
                      date: Date)
