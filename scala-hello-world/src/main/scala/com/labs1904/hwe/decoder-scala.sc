//open with the scala decoder file in de-hours-with-experts

//go through characters given and use as keys to find values

def decodeString(str: String): String = {
  str.map((ch:Char)=>{
    val key = ch.toString
    ENCODING.get(key) match{ //class established in original file -- could use ENCODING.getOrElse(key.toLowerCase,key)
      case Some(value) => value
      case None => key
    }
  }).mkString("")
}

def decodeIngredient(line: String): Ingredient ={ //established in original file)
  //split ingredient on #
  //use decodeString function -- ingredients.map(ingredient => decodeString(ingredient))

}

//parentheses instead of square brackets to find item at index -- decoded(0)