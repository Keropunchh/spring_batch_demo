package com.example.batchprocessing;

import java.util.Date;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.WritableResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@Configuration
public class BatchConfiguration {

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private JobRepository jobRepository;

	@Bean
	public FlatFileItemReader<Employee> reader() {
		return new FlatFileItemReaderBuilder<Employee>()
				.name("employeeItemReader")
				.resource(new ClassPathResource("sample-data.csv"))
				.delimited()
				.names("name", "hourlyRate", "hoursWorked")
				.targetType(Employee.class)
				.build();
	}

	@Bean
	public EmployeeItemProcessor processor() {
		return new EmployeeItemProcessor();
	}

	@Bean
	public FlatFileItemWriter<EmployeeSalary> writer() {
		return new FlatFileItemWriterBuilder<EmployeeSalary>()
				.name("employeeSalaryItemWriter")
				.resource(new FileSystemResource("src/main/resources/sample-salary.csv"))
				.delimited()
				.delimiter(",") // Specify delimiter for the CSV file
				.names("name", "annualSalary") // Map the fields from Employee Salary class
				.build();
	}

	@Bean
	public Job importUserJob(JobRepository jobRepository, Step step1, JobCompletionNotificationListener listener) {
		return new JobBuilder("importUserJob", jobRepository)
				.listener(listener)
				.start(step1)
				.build();
	}

	@Bean
	public Step step1(JobRepository jobRepository, DataSourceTransactionManager transactionManager,
			FlatFileItemReader<Employee> reader, EmployeeItemProcessor processor,
			FlatFileItemWriter<EmployeeSalary> writer) {
		return new StepBuilder("step1", jobRepository)
				.<Employee, EmployeeSalary>chunk(3, transactionManager)
				.reader(reader)
				.processor(processor)
				.writer(writer)
//				.faultTolerant()
//                .retryLimit(3)
//                .retry(ProcessingException.class)
				.build();
	}

	// Schedule the job to run monthly on the 1st day at midnight
	@Scheduled(cron = "*/5 * * * * ?") // For every 5 seconds
	public void scheduleMonthlyJob() throws Exception {
		try {
	        JobParameters jobParameters = new JobParametersBuilder()
//	        	.addDate("startDate", new Date()) // Add a unique parameter to ensure job is run each time
	            .addLong("currentTime", System.currentTimeMillis())  // Use current time as unique parameter
	            .toJobParameters();

	        // Launch the batch job
	        jobLauncher.run(importUserJob(jobRepository,
	            step1(jobRepository, null, reader(), processor(), writer()), null),
	            jobParameters);
	    } catch (Exception e) {
	        // Log the exception
	        System.err.println("Job failed to execute: " + e.getMessage());
	    }
		
	}
}
