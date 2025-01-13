package io.prophecy.pipelines.automationpbt_v2scalafalse11.config

import org.apache.spark.sql.SparkSession
case class Context(spark: SparkSession, config: Config)
