package com.learnkafka.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

/**
 * Library event POJO which is passed in the kafka topic
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class LibraryEvent {

	private Integer libraryEventId;
	private LibraryEventType libraryEventType;
	@NotNull
	@Valid
	private Book book;

}
