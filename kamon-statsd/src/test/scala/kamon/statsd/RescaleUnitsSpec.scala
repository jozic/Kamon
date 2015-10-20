package kamon.statsd

import com.typesafe.config.ConfigFactory
import kamon.metric.instrument.{Memory, Time}
import org.scalatest.{Matchers, WordSpec}

class RescaleUnitsSpec extends WordSpec with Matchers {

  "RescaleUnits" should {
    "read time unit to rescale to from config and rescale properly" in {
      val rescaler = new RescaleUnits(
        ConfigFactory.parseString( """
                                     |rescale-time-to = "ms"
                                   """.stripMargin))

      rescaler.rescaleMemoryTo should be(None)
      rescaler.rescaleTimeTo should be(Some(Time.Milliseconds))

      rescaler.rescale(Time.Nanoseconds, 1000000) should be(1)
      rescaler.rescale(Memory.Bytes, 1000000) should be(1000000)
    }
    "read memory unit to rescale to from config and rescale properly" in {
      val rescaler = new RescaleUnits(
        ConfigFactory.parseString( """
                                     |rescale-memory-to = "kb"
                                   """.stripMargin))

      rescaler.rescaleMemoryTo should be(Some(Memory.KiloBytes))
      rescaler.rescaleTimeTo should be(None)

      rescaler.rescale(Time.Nanoseconds, 1000000) should be(1000000)
      rescaler.rescale(Memory.Bytes, 10240) should be(10)
    }
  }

}
