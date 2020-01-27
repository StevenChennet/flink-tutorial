package com.stevenchennet.net.flink191

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Table,Types}
import org.apache.flink.table.api.java.StreamTableEnvironment
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.apache.flink.types.Row

object KafkaProtobufSchemaSource {
  /**
   * 添加时间信息
   *
   * @param originTable
   * @param timeColumnName 时间列名称
   * @param delaySeconds   延时时间(秒)
   * @param ste
   * @return
   */
  def assginTimestampToSchemaTable(originTable: Table, timeColumnName: Option[String], delaySeconds: Option[Long], ste: StreamTableEnvironment): Table = {
    if (timeColumnName.isDefined && timeColumnName.get.trim.length > 0) {
      val timeColumnIndex = originTable.getSchema.getFieldNames.indexOf(timeColumnName.get)

      val targetStream = ste.toAppendStream(originTable, classOf[Row])
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Row](Time.seconds(delaySeconds.getOrElse(0))) {
          override def extractTimestamp(element: Row): Long = element.getField(timeColumnIndex).asInstanceOf[java.sql.Timestamp].getTime
        })

      val targetTableTypeInformations: Array[TypeInformation[_]] = new Array[TypeInformation[_]](originTable.getSchema.getFieldTypes.length)
      originTable.getSchema.getFieldTypes.copyToArray(targetTableTypeInformations)
      targetTableTypeInformations(timeColumnIndex) = TimeIndicatorTypeInfo.ROWTIME_INDICATOR

      import collection.JavaConverters._

      val rowTypeInfo = Types.ROW(originTable.getSchema.getFieldNames, targetTableTypeInformations)
      //val rowTypeInfo = Types.ROW(targetTableTypeInformations， originTable.getSchema.getFieldNames)
      val typeTargetStream = targetStream.returns(rowTypeInfo)
      val targetTable = ste.fromDataStream(typeTargetStream)
      println("----assginTimestampToSchemaTable----targetTable----")
      targetTable.printSchema()
      targetTable
    } else {
      originTable
    }
  }
}
