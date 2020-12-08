package kz.bigdata.packages.triggers

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{DataFrame, SaveMode}
import kz.dmc.packages.spark.DMCSpark


object Globals {
  def getStringParam(param: String): String = {
    val result = DMCSpark.get.getAppParam(param).getAsString
    if (result == null) throw new NullPointerException("Не найден параметр: " + param)
    result
  }

  def getKafkaParams(): Map[String, Object] = {
    val kafkaServer = getStringParam("kafka.server")
    val inGroupId = getStringParam("kafka.in.groupid")
    val inUsername = getStringParam("kafka.in.username")
    val inPassword = getStringParam("kafka.in.password")

    Map[String, Object](
      "bootstrap.servers" -> kafkaServer,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> inGroupId,
      "auto.offset.reset" -> "earliest",
      "auto.commit.interval.ms" -> "1000",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "security.protocol" -> "SASL_PLAINTEXT",
      "sasl.mechanism" -> "PLAIN",
      "sasl.jaas.config" -> "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";".format(inUsername, inPassword)
    )
  }

  def saveToKafka(df: DataFrame): Unit = {
    val kafkaServer = getStringParam("kafka.server")
    val outTopicId = getStringParam("kafka.out.topic")
    val outUsername = getStringParam("kafka.out.username")
    val outPassword = getStringParam("kafka.out.password")

    df.toJSON
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServer)
      .option("topic", outTopicId)
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.security.protocol", "SASL_PLAINTEXT")
      .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";".format(outUsername, outPassword))
      .save()
  }

  def saveToDatabase(df: DataFrame): Unit = {
    val jdbcDriver = getStringParam("jdbc.out.driver")
    val jdbcURL = getStringParam("jdbc.out.url")
    val jdbcTable = getStringParam("jdbc.out.table")
    val jdbcUsername = getStringParam("jdbc.out.username")
    val jdbcPassword = getStringParam("jdbc.out.password")

    df.write
      .format("jdbc")
      .mode(SaveMode.Append)
      .option("driver", jdbcDriver)
      .option("url", jdbcURL)
      .option("dbtable", jdbcTable)
      .option("user", jdbcUsername)
      .option("password", jdbcPassword)
      .save()
  }
}
