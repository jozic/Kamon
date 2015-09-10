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
import akka.util.ByteString
import com.typesafe.config.Config
import kamon.metric.SubscriptionsDispatcher.TickMetricSnapshot

import kamon.metric.instrument.{ Counter, Histogram }

class SimpleStatsDMetricsSender(config: Config, metricKeyGenerator: MetricKeyGenerator)
    extends BaseStatsDMetricsSender(config, metricKeyGenerator) {

  val configSettings = config.getConfig("kamon.statsd.simple-metric-sender")
  val statsDHost = configSettings.getString("hostname")
  val statsDPort = configSettings.getInt("port")

  def writeMetricsToRemote(tick: TickMetricSnapshot, udpSender: ActorRef): Unit = {

    def flushToUDP(data: String): Unit = udpSender ! Udp.Send(ByteString(data), newSocketAddress)

    for (
      (entity, snapshot) ← tick.metrics;
      (metricKey, metricSnapshot) ← snapshot.metrics
    ) {

      val key = metricKeyGenerator.generateKey(entity, metricKey)
      val keyPrefix = key + ":"

      metricSnapshot match {
        case hs: Histogram.Snapshot ⇒
          hs.recordsIterator.foreach { record ⇒
            flushToUDP(keyPrefix + encodeStatsDTimer(record.level, record.count))
          }

        case cs: Counter.Snapshot ⇒
          flushToUDP(keyPrefix + encodeStatsDCounter(cs.count))
      }
    }
  }
}

object SimpleStatsDMetricsSender extends StatsDMetricsSenderFactory {
  override def props(config: Config, metricKeyGenerator: MetricKeyGenerator): Props =
    Props(new SimpleStatsDMetricsSender(config, metricKeyGenerator))
}
