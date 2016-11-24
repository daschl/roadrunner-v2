package com.couchbase.roadrunner.customConverter;


import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import java.io.IOException;
import java.text.SimpleDateFormat;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

public class JacksonConverter {
	private final ObjectMapper mapper =
			new ObjectMapper()
					.configure(FAIL_ON_UNKNOWN_PROPERTIES, false)
					.setSerializationInclusion(JsonInclude.Include.NON_NULL)
					.enable(SerializationFeature.INDENT_OUTPUT)
					.setDateFormat(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ"));

	private final ObjectReader reader = mapper.reader();
	private final ObjectWriter writer = mapper.writer();

	public <T> T fromBytes(byte[] source, Class<T> valueType) {
		try {
			return reader.forType(valueType).readValue(source);
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	public <T> byte[] toBytes(T source) {
		try {
			return writer.writeValueAsBytes(source);
		} catch (JsonProcessingException e) {
			throw new IllegalStateException(e);
		}
	}
}