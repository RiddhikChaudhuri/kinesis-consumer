package com.kinesis.consumer.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.kinesis.consumer.dto.User;
import com.kinesis.consumer.repository.UserRepository;
import com.kinesis.consumer.service.UserService;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

@Service
public class RecordProcessor implements ShardRecordProcessor {

	private static final Logger log = LoggerFactory.getLogger(RecordProcessor.class);

	@Autowired
	private UserService userService;
	
	
	
	private UserRepository userRepository;

    private final Gson gson = new Gson();


	public RecordProcessor(UserRepository userRepository) {
		this.userRepository=userRepository;
	}

	@Override
	public void initialize(InitializationInput initializationInput) {
		log.info("Initialization complete");
	}

	@Override
	public void processRecords(ProcessRecordsInput processRecordsInput) {

		// Data is read here from the Kinesis data stream
		for (KinesisClientRecord record : processRecordsInput.records()) {

			log.info("Processing Record For Partition Key : {}", record.partitionKey());

			String originalData = "";

			try {
				//byte[] b = new byte[record.data().duplicate().get()];
                originalData = SdkBytes.fromByteBuffer(record.data()).asUtf8String();


				log.info("Data from kinesis stream : {}", originalData);

				ObjectMapper objectMapper = new ObjectMapper();

				User user = objectMapper.readValue(originalData, User.class);
				userRepository.save(user);

			} catch (Exception e) {
				log.error("Error parsing record {}", e);
			}

			try {
				/*
				 * KCL assumes that the call to checkpoint means that all records have been
				 * processed, records which are passed to the record processor.
				 */
				processRecordsInput.checkpointer().checkpoint();

			} catch (Exception e) {
				log.error("Error during Processing of records", e);
			}
		}
	}

	@Override
	public void leaseLost(LeaseLostInput leaseLostInput) {
		log.error("LeaseLostInput {}", leaseLostInput);
	}

	@Override
	public void shardEnded(ShardEndedInput shardEndedInput) {
		try {
			shardEndedInput.checkpointer().checkpoint();
		} catch (ShutdownException | InvalidStateException e) {

			e.printStackTrace();
		}
	}

	@Override
	public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
		try {
			shutdownRequestedInput.checkpointer().checkpoint();
		} catch (ShutdownException | InvalidStateException e) {

			e.printStackTrace();
		}
	}

}