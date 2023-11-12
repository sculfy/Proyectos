// Databricks notebook source
//Importar las clases necesarias
import org.apache.spark.sql.SparkSession

//Creando el contexto de spark
val sc = spark.sparkContext

// COMMAND ----------

//Leemos la primera tabla
val dfwhr1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/proyecto/world_happiness_report.csv")
dfwhr1.show()
//también se puede hacer un display display(dfwhr1)

// COMMAND ----------

dfwhr1.printSchema

// COMMAND ----------

//Leemos la segunda tabla
val dfwhr2 = spark.read.option("header", "true").csv("dbfs:/FileStore/proyecto/world_happiness_report_2021.csv")
dfwhr2.show()
//también se puede hacer un display display(dfwhr2)

// COMMAND ----------

dfwhr2.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ### 1. ¿Cuál es el país más “feliz” del 2021 según la data? (considerar que la columna “Ladder score” mayor número más feliz es el país) 

// COMMAND ----------

//Importaciones
import org.apache.spark.sql.functions.{col, max}

//Cambio de tipos
val dfwhr2_2 = dfwhr2.withColumn("Ladder score", col("Ladder score").cast("Double"))

//Max Score
val maxScore = dfwhr2_2.select(max("Ladder score").cast("double")).collect()(0)(0)

//País más feliz
val happiestCountry = dfwhr2_2.filter(col("Ladder score") === maxScore).select("Country name").first()

//Result
println(s"$happiestCountry es el país más feliz en 2021 con un Score de $maxScore.")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 2. ¿Cuál es el país más “feliz” del 2021 por continente según la data? 

// COMMAND ----------

//Importaciones
import org.apache.spark.sql.functions.{col, max, desc}

//Cambio de tipos
val dfwhr2_2 = dfwhr2.withColumn("Ladder score", col("Ladder score").cast("Double"))

//Max Score
val maxScore = dfwhr2_2.groupBy("Regional indicator").agg(max(col("Ladder score")).as("Max_Score"))

//Pais más feliz por región
val happiestCountry = dfwhr2_2.join(maxScore, Seq("Regional indicator"), "inner")
    .where(col("Ladder score") === col("Max_Score"))
    .select("Regional indicator", "Country name", "Max_Score")

//Result
happiestCountry.orderBy(desc("Max_Score")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 3. ¿Cuál es el país que más veces ocupó el primer lugar en todos los años? 

// COMMAND ----------

//Importaciones
import org.apache.spark.sql.functions._

//AÑO 2021
//Cambio de tipos
val dfwhr2_2 = dfwhr2.withColumn("Ladder score", col("Ladder score").cast("Double")).withColumn("year", lit("2021"))
//Max Score 2021
val maxScore2021 = dfwhr2_2.groupBy("year").agg(max(col("Ladder score")).as("Max_Score"))
//País más feliz en 2021
val happiestCountry2021 = dfwhr2_2.join(maxScore2021, Seq("year"), "inner")
  .where(col("Ladder score") === col("Max_Score"))
  .select("Country name", "Max_Score")


//RESTO AÑOS
//Cambio de tipos
val dfwhr1_2 = dfwhr1.withColumn("Life Ladder", col("Life Ladder").cast("Double"))
//Max Score resto años
val maxScore = dfwhr1_2.groupBy("year").agg(max(col("Life Ladder")).as("Max_Score"))
//País más feliz resto años
val happiestCountry = dfwhr1_2.join(maxScore, Seq("year"), "inner")
  .where(col("Life Ladder") === col("Max_Score"))
  .select("Country name", "Max_Score")


//Unir los dos resultados
val dataFinal = happiestCountry2021.union(happiestCountry)

//display(dataFinal)

//Recuento
val recuento = dataFinal.groupBy("Country name").agg(count("*").as("Num Años Mas Feliz"))

//Pais TOP
val recuentoMax = recuento.agg(max("Num Años Mas Feliz")).first().getLong(0)
val paisTop = recuento.filter(col("Num Años Mas Feliz") === recuentoMax)

//Result
paisTop.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### 4. ¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020? 

// COMMAND ----------

//Importaciones
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

//Cambio de tipos
val dfwhr1_2 = dfwhr1.withColumn("Life Ladder", col("Life Ladder").cast("Double"))
                      .withColumn("Log GDP per capita",col("Log GDP per capita").cast("Double"))

//Filtro 2020
val df2020 = dfwhr1_2.filter(col("year")==="2020")

//display(df2020)

//Indice
val dfindex= df2020.withColumn("index",row_number().over(Window.orderBy(col("Life Ladder").desc)))

//display(dfindex)

//Max GDP
val gdpMax = dfindex.select(max("Log GDP per capita")).collect()(0)(0)

//Pais con mas gdp
val countryMaxGdp = dfindex.filter(col("Log GDP per capita") === gdpMax).select("Country name").first()

//Felicidad del pais con mas gdp
val countryIndex = dfindex.filter(col("Log GDP per capita") === gdpMax).select("index").first()

// Mostrar el resultado
println(s"$countryMaxGdp, el país con mas GDP de 2020, tiene el puesto $countryIndex de Felicidad.")

// COMMAND ----------

// MAGIC %md
// MAGIC ### 5. ¿En que porcentaje a variado a nivel mundial el GDP promedio del 2020 respecto al 2021? ¿Aumentó o disminuyó? 

// COMMAND ----------

//Importaciones
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

//Cambio de tipos
val dfwhr1_2 = dfwhr1.withColumn("Log GDP per capita",col("Log GDP per capita").cast("Double"))

//Filtro 2020
val df2020 = dfwhr1_2.filter(col("year")==="2020")

//Promedio 2020
val avgGdp2020 = df2020.groupBy("year").agg(avg(col("Log GDP per capita").as("Average GDP2020")))

//Cambio de tipos
val dfwhr2_2 = dfwhr2.withColumn("Logged GDP per capita", col("Logged GDP per capita").cast("Double")).withColumn("year", lit("2021"))

//Promedio 2021
val avgGdp2021 = dfwhr2_2.groupBy("year").agg(avg(col("Logged GDP per capita").as("Average GDP2021")))

//Variación 
val varGdp = ((avgGdp2021.collect()(0)(1).asInstanceOf[Double] - avgGdp2020.collect()(0)(1).asInstanceOf[Double]) / avgGdp2020.collect()(0)(1).asInstanceOf[Double]) * 100


//Result
if (varGdp < 0) {
                  val varFormat = f"%%.2f".format(varGdp)
                  println(s"El GDP, a nivel mundial, disminuyó un $varFormat% de 2020 a 2021.")
                } 
else if (varGdp > 0) {
                        val varFormat = f"%%.2f".format(varGdp)
                        println(s"El GDP, a nivel mundial, aumentó un $varFormat% de 2020 a 2021.")
                      } 
else {
        println("El GDP, a nivel mundial, se mantuvo de 2020 a 2021.")
      }

// COMMAND ----------

// MAGIC %md
// MAGIC ### 6. ¿Cuál es el país con mayor expectativa de vida (“Healthy life expectancy at birth”)? Y ¿Cuánto tenia en ese indicador en el 2019? 

// COMMAND ----------

//Importaciones
import org.apache.spark.sql.functions._

//Cambio de tipos
val dfwhr1_2 = dfwhr1.withColumn("Healthy life expectancy", col("Healthy life expectancy at birth").cast("Double"))
//Filtro 2019 & 2020
val dfhealthy = dfwhr1_2.select(col("year"),col("Healthy life expectancy"),col("Country name"))
val df2019 = dfhealthy.filter(col("year")==="2019")
val df2020 = dfhealthy.filter(col("year")==="2020")
//Cambio de tipos 2021
val dfwhr2_2 = dfwhr2.withColumn("Healthy life expectancy", col("Healthy life expectancy").cast("Double")).withColumn("year", lit("2021"))
val df2021 = dfwhr2_2.select(col("year"),col("Healthy life expectancy"),col("Country name"))

//Data 2020 & 2021
val df2020_2021 = df2020.union(df2021)

//Mayor Esperanza 2020 & 2021
val maxHealthy = df2020_2021.select(max("Healthy life expectancy").cast("double")).collect()(0)(0)

//Pais Mayor Esperanza 2020 & 2021
val countryMaxHealthy = df2020_2021.filter(col("Healthy life expectancy") === maxHealthy).select("Country name").first().getAs[String]("Country name")

//Esperanza 2019
val healthy2019 = df2019.filter(col("Country name")===countryMaxHealthy).select("Healthy life expectancy").first()

//Result
println(s"El pais con mayor esperanza de vida de 2020 & 2021 fue $countryMaxHealthy, con un indicador de $maxHealthy frente a $healthy2019 de 2019.")
