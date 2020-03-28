package covid19.nytimes.processor

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class NytimesBatchProcessorApplication

fun main(args: Array<String>) {
	runApplication<NytimesBatchProcessorApplication>(*args)
}
