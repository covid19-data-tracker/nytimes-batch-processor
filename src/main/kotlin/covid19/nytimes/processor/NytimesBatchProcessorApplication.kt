package covid19.nytimes.processor

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.core.step.tasklet.TaskletStep
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder
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
import java.sql.PreparedStatement
import java.util.*
import javax.sql.DataSource

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
class NytimesBatchProcessorApplication {
	companion object {
		const val USA_COUNTIES_TABLE = "covid19_usa_by_counties"
		const val USA_STATES_TABLE = "covid19_usa_by_states"
		val RUNTIME = System.currentTimeMillis()
	}
}

fun main(args: Array<String>) {
	val newArgs = arrayOf(*args, "--nytimes.runtime=${NytimesBatchProcessorApplication.RUNTIME}")
	runApplication<NytimesBatchProcessorApplication>(*newArgs)
}

fun intOrNull(str: String?) =
		if (str != null && str.trim() != "") Integer.parseInt(str) else null

fun parseDateString(str: String) =
		Calendar
				.getInstance()
				.apply {
					val nums = str.split("-").map { Integer.parseInt(it) }
					set(Calendar.YEAR, nums[0])
					set(Calendar.DAY_OF_MONTH, nums[2])
					set(Calendar.MONTH, nums[1])
				}
				.time

@Configuration
class JobConfiguration(
		private val nytProcessorProperties: NytProcessorProperties,
		private val jobBuilderFactory: JobBuilderFactory,
		private val statesStepConfiguration: StateStepConfiguration,
		private val countyStep: CountyStepConfiguration) {

	@Bean
	fun job() = this.jobBuilderFactory["nytimes-${nytProcessorProperties.runtime}"]
			.start(countyStep.stepBean())
			.next(statesStepConfiguration.stepBean())
			.build()
}

open class NytDataStepBaseConfiguration<T>(
		private val ds: DataSource,
		private val stepBuilderFactory: StepBuilderFactory,
		private val fieldSetMapper: FieldSetMapper<T>,
		private val resource: Resource,
		private val preparedStatementSetter: (T, PreparedStatement) -> Unit,
		private val sql: String,
		private val name: String) {

	open fun readerBean(): ItemReader<T> = this.buildReaderFor(this.resource, this.name, this.fieldSetMapper)
	open fun writerBean(): ItemWriter<T> = this.buildWriterFor(this.ds, this.sql, this.preparedStatementSetter)
	open fun stepBean(): TaskletStep = this.stepBuilderFactory[this.name].chunk<T, T>(1000).reader(this.readerBean()).writer(this.writerBean()).build()

	private fun <T> buildWriterFor(ds: DataSource, sql: String, pss: (T, PreparedStatement) -> Unit): ItemWriter<T> =
			JdbcBatchItemWriterBuilder<T>()
					.dataSource(ds)
					.assertUpdates(false)
					.itemPreparedStatementSetter(pss)
					.sql(sql)
					.build()

	private fun <T> buildReaderFor(resource: Resource, description: String, fsm: FieldSetMapper<T>): FlatFileItemReader<T> {
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

}

@Configuration
class CountyStepConfiguration(dataSource: DataSource, stepBuilderFactory: StepBuilderFactory, props: NytProcessorProperties) :
		NytDataStepBaseConfiguration<CountyBreakdown>(
				dataSource,
				stepBuilderFactory,
				FieldSetMapper {
					CountyBreakdown(parseDateString(it.readString(0)), it.readString(1), it.readString(2), intOrNull(it.readString(3)), it.readInt(4), it.readInt(5))
				},
				props.byCountyUrl, { state, ps ->
			ps.setDate(1, java.sql.Date(state.date.time))
			ps.setString(2, state.state)
			ps.setInt(3, state.fips ?: -1)
			ps.setInt(4, state.cases)
			ps.setInt(5, state.deaths)
			ps.setString(6, state.county)
		},
		"""
		insert into ${NytimesBatchProcessorApplication.USA_COUNTIES_TABLE}(date, state, fips, cases, deaths , county) values (?, ? , ? , ?, ?, ?)
		ON CONFLICT ON CONSTRAINT covid19_usa_by_counties_date_county_state_fips_key DO NOTHING
		""",
				NAME
		) {
	companion object {
		const val NAME = "counties"
	}

	@Bean("${NAME}-reader")
	override fun readerBean() = super.readerBean()

	@Bean("${NAME}-writer")
	override fun writerBean() = super.writerBean()

	@Bean("${NAME}-step")
	override fun stepBean() = super.stepBean()
}


@Configuration
class StateStepConfiguration(dataSource: DataSource, stepBuilderFactory: StepBuilderFactory, props: NytProcessorProperties) :
		NytDataStepBaseConfiguration<StateBreakdown>(
				dataSource,
				stepBuilderFactory,
				FieldSetMapper {
					StateBreakdown(parseDateString(it.readString(0)), it.readString(1), intOrNull(it.readString(2)), it.readInt(3), it.readInt(4))
				},
				props.byStateUrl,
				{ state, ps ->
					ps.setDate(1, java.sql.Date(state.date.time))
					ps.setString(2, state.state)
					ps.setInt(3, state.fips ?: -1)
					ps.setInt(4, state.cases)
					ps.setInt(5, state.deaths)
				},
		"""
	 	  INSERT INTO ${NytimesBatchProcessorApplication.USA_STATES_TABLE}(date, state, fips, cases, deaths) values (? , ? , ?, ?, ?) 
			ON CONFLICT ON CONSTRAINT covid19_usa_by_states_date_state_fips_key DO NOTHING
		""",
				NAME
	){

	companion object {
		const val NAME = "states"
	}

	@Bean("${NAME}-reader")
	override fun readerBean() = super.readerBean()

	@Bean("${NAME}-writer")
	override fun writerBean() = super.writerBean()

	@Bean("${NAME}-step")
	override fun stepBean() = super.stepBean()
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

@ConstructorBinding
@ConfigurationProperties("nytimes")
data class NytProcessorProperties(val byCountyUrl: Resource, val byStateUrl: Resource, val runtime: Long)