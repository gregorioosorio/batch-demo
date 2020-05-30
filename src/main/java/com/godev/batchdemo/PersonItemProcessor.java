package com.godev.batchdemo;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PersonItemProcessor implements ItemProcessor<Object, Person> {
	
	private StepExecution stepExecution;

	@Override
	public Person process(final Object object) throws Exception {
		Person person = new Person();
		if(object instanceof String) {
			stepExecution.getExecutionContext().putString("title", object.toString());
			return null;
		} else if(object instanceof Person) {
			Person p = (Person)object;
			person.setTitle(stepExecution.getExecutionContext().getString("title"));
			person.setFirstName(p.getFirstName());
			person.setLastName(p.getLastName());
		} else {
			return null;
		}
		final String title = person.getTitle().toUpperCase();
		final String firstName = person.getFirstName().toUpperCase();
		final String lastName = person.getLastName().toUpperCase();

		final Person transformedPerson = new Person(title, firstName, lastName);

		log.info("Converting (" + person + ") into (" + transformedPerson + ")");

		return transformedPerson;
	}

	
    @BeforeStep
    public void saveStepExecution(StepExecution stepExecution) {
        this.stepExecution = stepExecution;
    }
}
