/*
 * =========================================================================================
 * Copyright © 2013-2015 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package kamon.statsd

import akka.actor.{ Props, ActorRef }
import akka.io.Udp
import java.net.InetSocketAddress
import akka.util.ByteString
import com.typesafe.config.Config
import kamon.metric.SubscriptionsDispatcher.TickMetricSnapshot

import kamon.metric.instrument.{ Counter, Histogram }

object BatchStatsDMetricsSender extends StatsDMetricsSenderFactory {
  override def props(config: Config, metricKeyGenerator: MetricKeyGenerator): Props =
    Props(new BatchStatsDMetricsSender(config, metricKeyGenerator))
}

class BatchStatsDMetricsSender(config: Config, metricKeyGenerator: MetricKeyGenerator)
    extends BaseStatsDMetricsSender(config, metricKeyGenerator) {

  val configSettings = config.getConfig("kamon.statsd.batch-metric-sender")
  val maxPacketSizeInBytes = configSettings.getBytes("max-packet-size")
  val statsDHost = configSettings.getString("hostname")
  val statsDPort = configSettings.getInt("port")

  def writeMetricsToRemote(tick: TickMetricSnapshot, udpSender: ActorRef): Unit = {
    val packetBuilder = new MetricDataPacketBuilder(maxPacketSizeInBytes, udpSender, newSocketAddress)

    for (
      (entity, snapshot) ← tick.metrics;
      (metricKey, metricSnapshot) ← snapshot.metrics
    ) {

      val key = metricKeyGenerator.generateKey(entity, metricKey)

      metricSnapshot match {
        case hs: Histogram.Snapshot ⇒
          hs.recordsIterator.foreach { record ⇒
            packetBuilder.appendMeasurement(key, encodeStatsDTimer(record.level, record.count))
          }

        case cs: Counter.Snapshot ⇒
          packetBuilder.appendMeasurement(key, encodeStatsDCounter(cs.count))
      }
    }

    packetBuilder.flush()
  }
}

class MetricDataPacketBuilder(maxPacketSizeInBytes: Long, udpSender: ActorRef, remote: InetSocketAddress) {
  val metricSeparator = "\n"
  val measurementSeparator = ":"

  var lastKey = ""
  var buffer = new StringBuilder()

  def appendMeasurement(key: String, measurementData: String): Unit = {
    if (key == lastKey) {
      val dataWithoutKey = measurementSeparator + measurementData
      if (fitsOnBuffer(dataWithoutKey))
        buffer.append(dataWithoutKey)
      else {
        flushToUDP(buffer.toString())
        buffer.clear()
        buffer.append(key).append(dataWithoutKey)
      }
    } else {
      lastKey = key
      val dataWithoutSeparator = key + measurementSeparator + measurementData
      if (fitsOnBuffer(metricSeparator + dataWithoutSeparator)) {
        val mSeparator = if (buffer.length > 0) metricSeparator else ""
        buffer.append(mSeparator).append(dataWithoutSeparator)
      } else {
        flushToUDP(buffer.toString())
        buffer.clear()
        buffer.append(dataWithoutSeparator)
      }
    }
  }

  def fitsOnBuffer(data: String): Boolean = (buffer.length + data.length) <= maxPacketSizeInBytes

  private def flushToUDP(data: String): Unit = udpSender ! Udp.Send(ByteString(data), remote)

  def flush(): Unit = {
    flushToUDP(buffer.toString)
    buffer.clear()
  }
}