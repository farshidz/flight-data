package com.quantexa.flightdata

import java.sql.Date

case class FlightData(passengerId: Long,
                      flightId: Long,
                      from: String,
                      to: String,
                      date: Date)
