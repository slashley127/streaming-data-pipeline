//create a map containing 3-5 states
//key is state code
//value is place visited
//created list of codes in order you visited
//convert list from codes to a list of places you visited in order

val placesVisited = Map(
  "MO" -> "City Musuem",
  "OR" -> "Voodoo Donuts",
  "FL" -> "Manatee Park"
)

val placesInOrder = List(
  "MO", "OR", "FL"
)

println(placesInOrder.flatMap(code=>placesVisited.get(code)))
//.get returns and automatic Option
placesVisited.get("MO")
//returns Option[String] = Some(City Museum)
//saying that "MO" has a value attached to it

val pet: Option[String] = Some("Cat")

