package com.labs1904.hwe.practice

case class Item(description: String, price: Option[Int])

case class WeatherStation(name: String, temperature: Option[Int])

object OptionEither {
  /*
    Returns age of a dog when given a human age.
    Returns None if the input is None.
  */
  def dogAge(humanAge: Option[Int]): Option[Int] = {
    if(humanAge.isEmpty){
      None
    } else {
      Some(humanAge.get * 7)
    }
  }

  /*
    Returns the total cost af any item.
    If that item has a price, then the price + 7% of the price should be returned.
  */
  def totalCost(item: Item): Option[Double] = {
    if(item.price.isEmpty){
      None
    } else {
      Some((item.price.get*0.07)+item.price.get)
    }
  }

  /*
    Given a list of weather temperatures, calculates the average temperature across all weather stations.
    Some weather stations don't report temperature
    Returns None if the list is empty or no weather stations contain any temperature reading.
   */
  def averageTemperature(temperatures: List[WeatherStation]): Option[Int] = {
    val validTemps = temperatures.flatMap(_.temperature) //flatMap used to extract temps that are defined and return them in a new list (need to define as new variable (because makes new list))
    if(validTemps.nonEmpty)
      Some(validTemps.sum/validTemps.size)
    else None
  }
}
