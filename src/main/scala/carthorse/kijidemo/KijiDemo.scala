package carthorse.kijidemo

import org.kiji.schema.util.{FromJson, InstanceBuilder}
import org.kiji.schema.{KijiURI, Kiji}
import org.kiji.schema.layout.{KijiTableLayouts, KijiTableLayout}
import org.kiji.schema.avro.TableLayoutDesc

class KijiDemo {

  val layoutString =
    """{
      |  name: "table",
      |  keys_format : {
      |    encoding : "FORMATTED",
      |    salt : {
      |      hash_size : 2
      |    },
      |    components : [ {
      |      name : "first",
      |      type : "STRING"
      |    }, {
      |      name : "last",
      |      type : "STRING"
      |    } ]
      |  },
      |  locality_groups : [ {
      |    name : "default",
      |    in_memory : false,
      |    max_versions : 1000,
      |    ttl_seconds : 2147483647,
      |    compression_type : "GZ",
      |    families : [ {
      |      name : "group",
      |      columns : [ {
      |        name : "qual0",
      |        column_schema : {
      |          storage : "UID",
      |          type : "AVRO",
      |          avro_validation_policy : "DEVELOPER",
      |          readers : [],
      |          writers : []
      |        }
      |      }, {
      |        name : "qual1",
      |        column_schema : {
      |          storage : "UID",
      |          type : "AVRO",
      |          avro_validation_policy : "DEVELOPER",
      |          readers : [],
      |          writers : []
      |        }
      |      } ]
      |    }, {
      |      name : "map",
      |      map_schema : {
      |        storage : "UID",
      |        type : "AVRO",
      |        avro_validation_policy : "DEVELOPER",
      |        readers : [],
      |        writers : []
      |      }
      |    } ]
      |  } ],
      |  version: "layout-1.3"
      |}
    """.stripMargin

  val layout = FromJson.fromJsonString(layoutString, TableLayoutDesc.SCHEMA$).asInstanceOf[TableLayoutDesc]


  def populateTable(kiji: Kiji) = {
    new InstanceBuilder(kiji)
      .withTable(layout)
        .withRow("abc", "def")
          .withFamily("group")
            .withQualifier("qual0")
              .withValue(1, "q0_1").withValue(2, "q0_2").withValue(3, "q0_3").withValue(4, "q0_4").withValue(5, "q0_5")
            .withQualifier("qual1")
              .withValue(1, "q1_1").withValue(2, "q1_2").withValue(3, "q1_3").withValue(4, "q1_4").withValue(5, "q1_5")
          .withFamily("map")
            .withQualifier("map1")
              .withValue(1, "m1_1").withValue(2, "m1_2").withValue(3, "m1_3").withValue(4, "m1_4").withValue(5, "m1_5")
            .withQualifier("map2")
              .withValue(1, "m2_1").withValue(2, "m2_2").withValue(3, "m2_3").withValue(4, "m2_4").withValue(5, "m2_5")
            .withQualifier("map3")
              .withValue(1, "m3_1").withValue(2, "m3_2").withValue(3, "m3_3").withValue(4, "m3_4").withValue(5, "m3_5")
            .withQualifier("map4")
              .withValue(1, "m4_1").withValue(2, "m4_2").withValue(3, "m4_3").withValue(4, "m4_4").withValue(5, "m4_5")
            .withQualifier("map5")
              .withValue(1, "m5_1").withValue(2, "m5_2").withValue(3, "m5_3").withValue(4, "m5_4").withValue(5, "m5_5")
        .withRow("abc", "ghi")
          .withFamily("group")
            .withQualifier("qual0")
              .withValue(1, "q0_1").withValue(2, "q0_2").withValue(3, "q0_3").withValue(4, "q0_4").withValue(5, "q0_5")
            .withQualifier("qual1")
              .withValue(1, "q1_1").withValue(2, "q1_2").withValue(3, "q1_3").withValue(4, "q1_4").withValue(5, "q1_5")
          .withFamily("map")
            .withQualifier("map1")
              .withValue(1, "m1_1").withValue(2, "m1_2").withValue(3, "m1_3").withValue(4, "m1_4").withValue(5, "m1_5")
            .withQualifier("map2")
              .withValue(1, "m2_1").withValue(2, "m2_2").withValue(3, "m2_3").withValue(4, "m2_4").withValue(5, "m2_5")
            .withQualifier("map3")
              .withValue(1, "m3_1").withValue(2, "m3_2").withValue(3, "m3_3").withValue(4, "m3_4").withValue(5, "m3_5")
            .withQualifier("map4")
              .withValue(1, "m4_1").withValue(2, "m4_2").withValue(3, "m4_3").withValue(4, "m4_4").withValue(5, "m4_5")
            .withQualifier("map5")
              .withValue(1, "m5_1").withValue(2, "m5_2").withValue(3, "m5_3").withValue(4, "m5_4").withValue(5, "m5_5")
        .withRow("abc", "jkl")
          .withFamily("group")
            .withQualifier("qual0")
              .withValue(1, "q0_1").withValue(2, "q0_2").withValue(3, "q0_3").withValue(4, "q0_4").withValue(5, "q0_5")
            .withQualifier("qual1")
              .withValue(1, "q1_1").withValue(2, "q1_2").withValue(3, "q1_3").withValue(4, "q1_4").withValue(5, "q1_5")
          .withFamily("map")
            .withQualifier("map1")
              .withValue(1, "m1_1").withValue(2, "m1_2").withValue(3, "m1_3").withValue(4, "m1_4").withValue(5, "m1_5")
            .withQualifier("map2")
              .withValue(1, "m2_1").withValue(2, "m2_2").withValue(3, "m2_3").withValue(4, "m2_4").withValue(5, "m2_5")
            .withQualifier("map3")
              .withValue(1, "m3_1").withValue(2, "m3_2").withValue(3, "m3_3").withValue(4, "m3_4").withValue(5, "m3_5")
            .withQualifier("map4")
              .withValue(1, "m4_1").withValue(2, "m4_2").withValue(3, "m4_3").withValue(4, "m4_4").withValue(5, "m4_5")
            .withQualifier("map5")
              .withValue(1, "m5_1").withValue(2, "m5_2").withValue(3, "m5_3").withValue(4, "m5_4").withValue(5, "m5_5")
        .withRow("abc", "mno")
          .withFamily("group")
            .withQualifier("qual0")
              .withValue(1, "q0_1").withValue(2, "q0_2").withValue(3, "q0_3").withValue(4, "q0_4").withValue(5, "q0_5")
            .withQualifier("qual1")
              .withValue(1, "q1_1").withValue(2, "q1_2").withValue(3, "q1_3").withValue(4, "q1_4").withValue(5, "q1_5")
          .withFamily("map")
            .withQualifier("map1")
              .withValue(1, "m1_1").withValue(2, "m1_2").withValue(3, "m1_3").withValue(4, "m1_4").withValue(5, "m1_5")
            .withQualifier("map2")
              .withValue(1, "m2_1").withValue(2, "m2_2").withValue(3, "m2_3").withValue(4, "m2_4").withValue(5, "m2_5")
            .withQualifier("map3")
              .withValue(1, "m3_1").withValue(2, "m3_2").withValue(3, "m3_3").withValue(4, "m3_4").withValue(5, "m3_5")
            .withQualifier("map4")
              .withValue(1, "m4_1").withValue(2, "m4_2").withValue(3, "m4_3").withValue(4, "m4_4").withValue(5, "m4_5")
            .withQualifier("map5")
              .withValue(1, "m5_1").withValue(2, "m5_2").withValue(3, "m5_3").withValue(4, "m5_4").withValue(5, "m5_5")
        .withRow("abc", "pqr")
          .withFamily("group")
            .withQualifier("qual0")
              .withValue(1, "q0_1").withValue(2, "q0_2").withValue(3, "q0_3").withValue(4, "q0_4").withValue(5, "q0_5")
            .withQualifier("qual1")
              .withValue(1, "q1_1").withValue(2, "q1_2").withValue(3, "q1_3").withValue(4, "q1_4").withValue(5, "q1_5")
          .withFamily("map")
            .withQualifier("map1")
              .withValue(1, "m1_1").withValue(2, "m1_2").withValue(3, "m1_3").withValue(4, "m1_4").withValue(5, "m1_5")
            .withQualifier("map2")
              .withValue(1, "m2_1").withValue(2, "m2_2").withValue(3, "m2_3").withValue(4, "m2_4").withValue(5, "m2_5")
            .withQualifier("map3")
              .withValue(1, "m3_1").withValue(2, "m3_2").withValue(3, "m3_3").withValue(4, "m3_4").withValue(5, "m3_5")
            .withQualifier("map4")
              .withValue(1, "m4_1").withValue(2, "m4_2").withValue(3, "m4_3").withValue(4, "m4_4").withValue(5, "m4_5")
            .withQualifier("map5")
              .withValue(1, "m5_1").withValue(2, "m5_2").withValue(3, "m5_3").withValue(4, "m5_4").withValue(5, "m5_5")
    .build
  }


  def main(args: Array[String]) {
    val kiji = Kiji.Factory.open(KijiURI.newBuilder("kiji://.env/").withInstanceName("default").build)
    populateTable(kiji)

    // example usage goes here.
  }
}
