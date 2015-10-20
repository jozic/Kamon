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

import java.net.InetSocketAddress
import java.text.{ DecimalFormat, DecimalFormatSymbols }
import java.util.Locale
import akka.actor.{ Actor, ActorRef, ActorSystem }
import akka.io.{ IO, Udp }
import akka.util.ByteString
import com.typesafe.config.Config
import kamon.metric.SubscriptionsDispatcher.TickMetricSnapshot
import kamon.metric.instrument.{Memory, UnitOfMeasurement, Time}
import kamon.util.ConfigTools._

class RescaleUnits(config: Config) {

  val RescaleTimeTo = "rescale-time-to"
  val RescaleMemoryTo = "rescale-memory-to"

  lazy val rescaleTimeTo: Option[Time] =
    if (config.hasPath(RescaleTimeTo)) Some(config.time(RescaleTimeTo)) else None

  lazy val rescaleMemoryTo: Option[Memory] =
    if (config.hasPath(RescaleMemoryTo)) Some(config.memory(RescaleMemoryTo)) else None

  def rescale(unit: UnitOfMeasurement, value: Long): Long = (unit, rescaleTimeTo, rescaleMemoryTo) match {
    case (from: Time, Some(to), _) ⇒ from.scale(to)(value).toLong
    case (from: Memory, _, Some(to)) ⇒ from.scale(to)(value).toLong
    case _ ⇒ value
  }

}

trait StatsDValueFormatters {

  val symbols = DecimalFormatSymbols.getInstance(Locale.US)
  symbols.setDecimalSeparator('.')
  // Just in case there is some weird locale config we are not aware of.

  // Absurdly high number of decimal digits, let the other end lose precision if it needs to.
  val samplingRateFormat = new DecimalFormat("#.################################################################", symbols)

  def encodeStatsDTimer(level: Long, count: Long): String = {
    val samplingRate: Double = 1D / count
    level.toString + "|ms" + (if (samplingRate != 1D) "|@" + samplingRateFormat.format(samplingRate) else "")
  }

  def encodeStatsDCounter(count: Long): String = count.toString + "|c"
}

/**
 * Base class for different StatsD senders utilizing UDP protocol. It implies use of one statsd server.
 * @param statsDConfig Config to read settings specific to this sender
 * @param metricKeyGenerator Key generator for all metrics sent by this sender
 */
abstract class UDPBasedStatsDMetricsSender(statsDConfig: Config, metricKeyGenerator: MetricKeyGenerator)
    extends Actor with UdpExtensionProvider with StatsDValueFormatters {

  import context.system

  val statsDHost = statsDConfig.getString("hostname")
  val statsDPort = statsDConfig.getInt("port")
  val rescaler = new RescaleUnits(statsDConfig)

  udpExtension ! Udp.SimpleSender

  lazy val socketAddress = new InetSocketAddress(statsDHost, statsDPort)

  def receive = {
    case Udp.SimpleSenderReady ⇒
      context.become(ready(sender))
  }

  def ready(udpSender: ActorRef): Receive = {
    case tick: TickMetricSnapshot ⇒
      writeMetricsToRemote(tick,
        (data: String) ⇒ udpSender ! Udp.Send(ByteString(data), socketAddress))
  }

  def writeMetricsToRemote(tick: TickMetricSnapshot, flushToUDP: String ⇒ Unit): Unit

}

trait UdpExtensionProvider {
  def udpExtension(implicit system: ActorSystem): ActorRef = IO(Udp)
}

