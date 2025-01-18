package io.prophecy.pipelines.automationpbt_v2scalafalse11

import io.prophecy.libs._
import io.prophecy.pipelines.automationpbt_v2scalafalse11.config._
import io.prophecy.pipelines.automationpbt_v2scalafalse11.functions.UDFs._
import io.prophecy.pipelines.automationpbt_v2scalafalse11.functions.PipelineInitCode._
import io.prophecy.pipelines.automationpbt_v2scalafalse11.graph._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import java.time._

object Main {

  def apply(context: Context): Unit = {
    val df_s3_source_dataset = s3_source_dataset(context)
    create_lookup_table(context, df_s3_source_dataset)
    val df_reformat_customer_data =
      reformat_customer_data(context, df_s3_source_dataset)
    val df_script_execution_confirmation =
      script_execution_confirmation(context, df_reformat_customer_data)
    val df_create_temp_view_and_select =
      create_temp_view_and_select(context, df_s3_source_dataset)
  }

  def main(args: Array[String]): Unit = {
    val config = ConfigurationFactoryImpl.getConfig(args)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("AutomationPBT_v2-scalafalse11")
      .enableHiveSupport()
      .getOrCreate()
    val context = Context(spark, config)
    spark.conf.set("prophecy.metadata.pipeline.uri",
                   "pipelines/AutomationPBT_v2-scalafalse11"
    )
    spark.conf.set("spark.default.parallelism",             "4")
    spark.conf.set("spark.sql.legacy.allowUntypedScalaUDF", "true")
    registerUDFs(spark)
    MetricsCollector.instrument(spark,
                                "pipelines/AutomationPBT_v2-scalafalse11"
    ) {
      apply(context)
    }
  }

}
