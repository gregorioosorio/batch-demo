package com.godev.batchdemo;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.mapping.PatternMatchingCompositeLineMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.FixedLengthTokenizer;
import org.springframework.batch.item.file.transform.LineTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.validation.BindException;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Bean
	public FlatFileItemReader<Object> reader() {
		return new FlatFileItemReaderBuilder<Object>()
				.name("personItemReader")
				.resource(new ClassPathResource("sample-data.csv"))
				.lineMapper(userFileLineMapper())
				.build();
	}

	@Bean
	public PatternMatchingCompositeLineMapper<Object> userFileLineMapper() {
		PatternMatchingCompositeLineMapper<Object> lineMapper = new PatternMatchingCompositeLineMapper<>();

		Map<String, LineTokenizer> tokenizers = new HashMap<>(2);
		tokenizers.put("T*", headerTokenizer());
		tokenizers.put("D*", detailTokenizer());

		lineMapper.setTokenizers(tokenizers);

		Map<String, FieldSetMapper<Object>> mappers = new HashMap<>(2);
		mappers.put("T*", headerFieldSetMapper());
		mappers.put("D*", detailFieldSetMapper());

		lineMapper.setFieldSetMappers(mappers);

		return lineMapper;
	}

	private LineTokenizer headerTokenizer() {
		FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();
		tokenizer.setColumns(new Range(1, 1), new Range(2, 10));
		tokenizer.setNames("indicator", "title");
		return tokenizer;
	}

	private LineTokenizer detailTokenizer() {
		FixedLengthTokenizer tokenizer = new FixedLengthTokenizer();
		tokenizer.setColumns(new Range(1, 1), new Range(2, 10), new Range(11, 20));
		tokenizer.setNames("indicator", "firstName", "lastName");
		return tokenizer;
	}

	private FieldSetMapper<Object> headerFieldSetMapper() {
		FieldSetMapper<Object> mapper = new FieldSetMapper<>() {

			@Override
			public Object mapFieldSet(FieldSet fieldSet) throws BindException {
				return fieldSet.readString("title");
			}
		};

		return mapper;
	}

	private FieldSetMapper<Object> detailFieldSetMapper() {
		FieldSetMapper<Object> mapper = new FieldSetMapper<>() {

			@Override
			public Object mapFieldSet(FieldSet fieldSet) throws BindException {
				return new Person("", fieldSet.readString("firstName"), fieldSet.readString("lastName"));
			}
		};

		return mapper;
	}

	@Bean
	public PersonItemProcessor processor() {
		return new PersonItemProcessor();
	}

	@Bean
	public JdbcBatchItemWriter<Person> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Person>()
				.itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
				.sql("INSERT INTO people (title, first_name, last_name) VALUES (:title, :firstName, :lastName)").dataSource(dataSource)
				.build();
	}

	@Bean
	public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
		return jobBuilderFactory.get("importUserJob").incrementer(new RunIdIncrementer()).listener(listener).flow(step1)
				.end().build();
	}

	@Bean
	public Step step1(JdbcBatchItemWriter<Person> writer) {
		return stepBuilderFactory.get("step1").<Object, Person>chunk(10).reader(reader()).processor(processor())
				.writer(writer).build();
	}
}