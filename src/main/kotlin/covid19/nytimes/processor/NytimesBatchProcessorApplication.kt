package covid19.nytimes.processor

import org.apache.commons.logging.LogFactory
import org.springframework.batch.core.JobParametersIncrementer
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.file.FlatFileItemReader
import org.springframework.batch.item.file.mapping.DefaultLineMapper
import org.springframework.batch.item.file.mapping.FieldSetMapper
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.Resource
import org.springframework.stereotype.Component
import java.util.*

/**
 *
 * This application pulls down the COVID19 cases by both states and counties.
 * The county data <a href"https://en.wikipedia.org/wiki/Federal_Information_Processing_Standards">shows information by the FIPS standard</a>.
 *
 * @author <a href="mailto:josh@joshlong.com">Josh Long</a>
 */
@SpringBootApplication
@EnableBatchProcessing
@EnableConfigurationProperties(NytProcessorProperties::class)
class NytimesBatchProcessorApplication

fun main(args: Array<String>) {
	runApplication<NytimesBatchProcessorApplication>(*args)
}

fun intOrNull(str: String?) = if (str != null && str.trim() != "") Integer.parseInt(str) else null

fun parseDateString(str: String): Date {
	val nums = str.split("-").map { Integer.parseInt(it) }
	val y = nums[0]
	val m = nums[1]
	val d = nums[2]
	return Date(y, m, d)
}

inline fun <reified T> buildReaderFor(resource: Resource, description: String, fsm: FieldSetMapper<T>): FlatFileItemReader<T> {
	val dlt = DelimitedLineTokenizer()
			.apply {
				setDelimiter(",")
				afterPropertiesSet()
			}
	val lm = DefaultLineMapper<T>()
			.apply {
				setLineTokenizer(dlt)
				setFieldSetMapper(fsm)
				afterPropertiesSet()
			}
	return FlatFileItemReader<T>()
			.apply {
				setResource(resource)
				setName("read-${description}")
				setLinesToSkip(1)
				setLineMapper(lm)
				afterPropertiesSet()
			}
}

@Configuration
class JobConfiguration(
		private val jobBuilderFactory: JobBuilderFactory,
		private val statesStepConfiguration: StatesStepConfiguration,
		private val countiesStep: CountiesStepConfiguration) {

	@Bean
	fun job() = this.jobBuilderFactory["nytimes"]
			.start(countiesStep.countiesStep())
			.next(statesStepConfiguration.statesStep())
			.build()
}


@Configuration
class CountiesStepConfiguration(
		private val stepBuilderFactory: StepBuilderFactory,
		private val nytProcessorProperties: NytProcessorProperties) {

	@Bean
	fun countiesReader() = buildReaderFor(
			this.nytProcessorProperties.byCountyUrl,
			"counties",
			FieldSetMapper {
				CountyBreakdown(parseDateString(it.readString(0)), it.readString(1), it.readString(2), intOrNull(it.readString(3)), it.readInt(4), it.readInt(5))
			}
	)

	@Bean
	fun countiesWriter() = ItemWriter<CountyBreakdown> { counties ->
		counties.forEach { println(it) }
	}

	@Bean
	fun countiesStep() =
			stepBuilderFactory["counties"]
					.chunk<CountyBreakdown, CountyBreakdown>(100)
					.reader(countiesReader())
					.writer(countiesWriter())
					.build()

}

@Configuration
class StatesStepConfiguration(
		private val stepBuilderFactory: StepBuilderFactory,
		private val nytProcessorProperties: NytProcessorProperties) {

	@Bean
	fun statesReader() = buildReaderFor(
			this.nytProcessorProperties.byStateUrl,
			"counties",
			FieldSetMapper {
				StateBreakdown(parseDateString(it.readString(0)), it.readString(1), intOrNull(it.readString(2)), it.readInt(3), it.readInt(4))
			}
	)

	@Bean
	fun statesWriter() = ItemWriter<StateBreakdown> { states ->
		states.forEach { println(it) }
	}

	@Bean
	fun statesStep() =
			stepBuilderFactory["counties"]
					.chunk<StateBreakdown, StateBreakdown>(100)
					.reader(statesReader())
					.writer(statesWriter())
					.build()

}


data class CountyBreakdown(val date: Date,
                           val county: String,
                           val state: String,
                           val fips: Int?,
                           val cases: Int,
                           val deaths: Int)

data class StateBreakdown(val date: Date,
                          val state: String,
                          val fips: Int?,
                          val cases: Int,
                          val deaths: Int)


@Component
class Runner(private val nytProcessorProperties: NytProcessorProperties) {

	private val log = LogFactory.getLog(Runner::class.java)

	@Bean
	fun go() {
		this.log.info("by-state: ${this.nytProcessorProperties.byStateUrl}")
		this.log.info("by-county: ${this.nytProcessorProperties.byStateUrl}")
	}
}


@ConstructorBinding
@ConfigurationProperties("nytimes")
data class NytProcessorProperties(val byCountyUrl: Resource, val byStateUrl: Resource)

