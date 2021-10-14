package com.kinesis.consumer.processor;



import org.springframework.stereotype.Component;

import com.kinesis.consumer.repository.UserRepository;

import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

@Component
public class RecordProcessorFactory implements ShardRecordProcessorFactory {

	
	private UserRepository userRepository;
	
	
    public RecordProcessorFactory(UserRepository userRepository2) {
    	this.userRepository = userRepository2;
	}


	@Override
    public ShardRecordProcessor shardRecordProcessor() {
        return new RecordProcessor(userRepository);
    }

}