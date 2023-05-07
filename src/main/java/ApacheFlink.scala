import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._

import java.io.IOException
import java.sql.DriverManager

object ApacheFlink {

  def main(args: Array[String]): Unit = {
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

    env.setParallelism(1)

    val kafkaSource = KafkaSource.builder()
      .setBootstrapServers("localhost:9092")
      .setTopics("wikimedia_recentchange2")
      .setGroupId("myGroup")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val serializer = KafkaRecordSerializationSchema.builder()
      .setValueSerializationSchema(new SimpleStringSchema())
      .setTopic("wikimedia_recentchange2")
      .build()

    val lines = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")

    val necessaryMessages = lines.filter(textLine => textLine.contains("\"bot\":true")).executeAndCollect()

    val connect = DriverManager.getConnection("jdbc:postgresql://localhost:5432/apache-flink")
    val statement = connect.createStatement()

    val time = System.nanoTime
    var count = 0

    necessaryMessages.foreach(unit => {
      try {
        count += 1
        val sql = s"INSERT INTO data (column2, column3) VALUES('${unit.substring(0, unit.indexOf("\n"))}', '${(System.nanoTime() - time) / 1e9d}')"
        statement.executeUpdate(sql)
      }
      catch {
        case c: IOException =>
          println("Данная операция была прервана " + c.printStackTrace())
      }
    })
//    println("///////" + count / ((System.nanoTime() - time) / 1e9d) + " tps")
  }
}