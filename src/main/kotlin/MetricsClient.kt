
import io.micrometer.core.instrument.Clock
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.HTTPServer
import uk.gov.dwp.dataworks.logging.DataworksLogger

object MetricsClient {

    private val logger = DataworksLogger.getLogger(MetricsClient::class)

    init {
        HTTPServer(8080).run {
            logger.info("Metrics server started", "port" to "$port")
        }
    }

    val batchTimer: Timer by lazy {
        meterRegistry.timer("k2hb.batch.times")
    }

    private val meterRegistry: MeterRegistry by lazy {
        PrometheusMeterRegistry(PrometheusConfig.DEFAULT, CollectorRegistry.defaultRegistry, Clock.SYSTEM).apply {
            Metrics.globalRegistry.add(this)
            config().commonTags("k2hb.topic.regex", Config.Kafka.topicRegex.pattern,
                "instance.name", Config.Metrics.instanceName)
        }
    }

}
