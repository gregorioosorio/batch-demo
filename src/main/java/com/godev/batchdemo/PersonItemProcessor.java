package com.godev.batchdemo;

import org.springframework.batch.item.ItemProcessor;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PersonItemProcessor implements ItemProcessor<Object, Person> {

	@Override
	public Person process(final Object object) throws Exception {
		Person person = new Person();
		if(object instanceof String) {
			person.setTitle(object.toString());
			person.setFirstName("Default");
			person.setLastName("Default");
		} else if(object instanceof Person) {
			Person p = (Person)object;
			person.setTitle("Default");
			person.setFirstName(p.getFirstName());
			person.setLastName(p.getLastName());
		}
		final String title = person.getTitle().toUpperCase();
		final String firstName = person.getFirstName().toUpperCase();
		final String lastName = person.getLastName().toUpperCase();

		final Person transformedPerson = new Person(title, firstName, lastName);

		log.info("Converting (" + person + ") into (" + transformedPerson + ")");

		return transformedPerson;
	}

}
