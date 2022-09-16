// MAGIC %md
// MAGIC Zadanie 2

// COMMAND ----------

val path = "/FileStore/tables/Nested.json"
val json = spark.read.option("multiline","true").json(path)
display(json)

// COMMAND ----------

val json_drop = json.withColumn("newCol", $"pathLinkInfo".dropFields("alternateName","sourceFID"))
                            

// COMMAND ----------

// MAGIC %md
// MAGIC 2a

// COMMAND ----------

class Person(val firstname: String, val lastname: String, val sex: Symbol)

object Person {
  def apply(firstname: String, lastname: String, sex: Symbol) = new Person(firstname, lastname, sex)
}

val persons = Person("Jan", "Kowalski", 'male) ::
              Person("Adam", "Nowak", 'male) ::
              Person("Oliwia", "Kapusta", 'female) ::
              Person("Magdalena", "Bazylak", 'female) ::
              Nil

val strings = persons.foldLeft(List[String]()) { (x, y) =>
  val title = y.sex match {
    case 'male => "Mr."
    case 'female => "Ms."
  }
  x :+ s"$title ${y.name}, ${y.age}"
}

// COMMAND ----------

strings(0)

// COMMAND ----------

val prices: Seq[Double] = Seq(1.5, 2.0, 2.5)
val sum = prices.foldLeft(0.0)(_ + _)

// COMMAND ----------

import org.apache.spark.sql.DataFrame

def deleteNestedFields(fields:Map):DataFrame = { 
    //json.foldLeft(json.withColumn("newCol", $.dropFields(fields:_*)) {
    //  (k, v) =>
    }
}

// COMMAND ----------

val excludedNestedFields = Map("pathLinkInfo" -> Array("alternateName","sourceFID"))