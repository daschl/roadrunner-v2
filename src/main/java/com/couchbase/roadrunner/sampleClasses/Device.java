package com.couchbase.roadrunner.sampleClasses;

/*
 * HPE SNAP 2015
 */
import java.time.LocalDateTime;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Device {

	@JsonProperty
	private String productFamilyId;

	@JsonProperty
	private int partition;

	@JsonProperty
	private long effectiveDate;

	@JsonProperty
	private long expirationDate;

	@JsonProperty
	private long lastSyncedTime;

	@JsonProperty
	private long cacheExpiredTIme;

	public Device() {
		productFamilyId =  "";
		partition = 0;
		effectiveDate = LocalDateTime.now().getDayOfYear();
		expirationDate = LocalDateTime.now().getDayOfYear();
		lastSyncedTime = System.currentTimeMillis();
		cacheExpiredTIme = 0;
	}

}
