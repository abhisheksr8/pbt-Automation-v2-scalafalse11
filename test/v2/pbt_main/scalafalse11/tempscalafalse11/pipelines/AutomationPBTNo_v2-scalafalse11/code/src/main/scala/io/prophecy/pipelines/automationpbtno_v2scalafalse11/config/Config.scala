package io.prophecy.pipelines.automationpbtno_v2scalafalse11.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(
  var c_string:  String = "string$$%^&*#@",
  var c_int:     Int = 65530,
  var c_boolean: Boolean = true
) extends ConfigBase
