package org.zencode.guru.app;

import java.io.File;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;

@SpringBootApplication
@EnableBatchProcessing
public class SpringBatchApplication {

	public static void main(String[] args) {
		
		System.setProperty("input", "file:///"+new File("empDB.csv").getAbsolutePath());
    	System.setProperty("output", new File("q.csv").getAbsolutePath());
		SpringApplication.run(SpringBatchApplication.class, args);
		
		
	}
	
	public static class Person {

		private Integer age;
		private String name;
		private String email;
		
		public Person() {
		}
		
		public Integer getAge() {
			return age;
		}
		public void setAge(Integer age) {
			this.age = age;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getEmail() {
			return email;
		}
		public void setEmail(String email) {
			this.email = email;
		}	
	}
	
	@Bean
	Job job(JobBuilderFactory jbf,StepBuilderFactory sbf
			,ItemReader<? extends Person> ir
			,ItemWriter<? super Person> iw) {
		
		Step s1 = sbf.get("file-db")
				.<Person,Person> chunk(100)
				.reader(ir)
				.writer(iw)
				.build();
			
		
		return jbf.get("etl")
			.incrementer(new RunIdIncrementer())
			.start(s1)
			.build();
		
	}
	
	@Bean
	JdbcBatchItemWriter<Person> jdbcWRiter(DataSource ds){
		return new JdbcBatchItemWriterBuilder<Person>()
				.dataSource(ds)
				.sql("insert into PERSON(NAME,AGE,EMAIL) values(:name,:age,:email)")
				.beanMapped()
				.build();
	}
	
	@Bean
	FlatFileItemReader<Person> fileReader(@Value("${input}") Resource in){
		return new FlatFileItemReaderBuilder<Person>()
				.resource(in)
				.name("file-reader")
				.targetType(Person.class)
				.delimited().delimiter(",").names(new String[] {"name","age","email"})
				.build();
	}
	
	
}
