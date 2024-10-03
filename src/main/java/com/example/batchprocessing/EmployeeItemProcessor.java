package com.example.batchprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

public class EmployeeItemProcessor implements ItemProcessor<Employee, EmployeeSalary>{
	
	private static final Logger log = LoggerFactory.getLogger(EmployeeItemProcessor.class);
	private int attemptCount = 0;

	@Override
	public EmployeeSalary process(final Employee employee) throws Exception {
//		if("Jill".equals(employee.name()) && attemptCount < 2) {
//			attemptCount++;
//			System.out.println("Processing attempt " + attemptCount + " failed for: " + employee.name());
//	        throw new ProcessingException("Forced failure for testing retry.");
//		}
		final String name = employee.name();
		final double annualSalary = employee.hourlyRate() * employee.hoursWorked();
		final EmployeeSalary employeeSalary = new EmployeeSalary(name, annualSalary);
		log.info("INFO : " + employeeSalary);
		return employeeSalary;
	}
}
