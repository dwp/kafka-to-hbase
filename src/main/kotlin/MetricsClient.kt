import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Metrics
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.prometheus.client.CollectorRegistry

object MetricsClient {

    val failedBatchPutCounter by lazy {
        meterRegistry.counter("failed.s3.puts", "topic", "partition")
    }

    private val meterRegistry: MeterRegistry by lazy {
        val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT, CollectorRegistry.defaultRegistry, Clock.SYSTEM)
        Metrics.globalRegistry.add(meterRegistry)
        meterRegistry
    }

}
