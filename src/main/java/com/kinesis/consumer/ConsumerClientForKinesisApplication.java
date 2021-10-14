package com.kinesis.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import com.kinesis.consumer.config.ConsumerConfig;
import com.kinesis.consumer.repository.UserRepository;

import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.InitialPositionInStream;
import software.amazon.kinesis.common.InitialPositionInStreamExtended;
import software.amazon.kinesis.coordinator.Scheduler;


@SpringBootApplication
public class ConsumerClientForKinesisApplication implements CommandLineRunner {
	
	private static final Logger LOG =
            LoggerFactory.getLogger(ConsumerClientForKinesisApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(ConsumerClientForKinesisApplication.class, args);
	}
	

    @Autowired
    private ApplicationContext context;
    
    @Autowired
    private UserRepository userRepository;
    
    @Override
    public void run(String... args) throws Exception {
        LOG.info("Running consumer application!");

        ConsumerConfig consumerConfig = context.getBean(ConsumerConfig.class);

        ConfigsBuilder configsBuilder = consumerConfig.getConfigBuilder(userRepository);

        /**
         * The Scheduler is the entry point to the KCL. This instance is configured with defaults
         * provided by the ConfigsBuilder.
         */
        Scheduler scheduler =
                new Scheduler(configsBuilder.checkpointConfig(), configsBuilder.coordinatorConfig(),
                        configsBuilder.leaseManagementConfig(), configsBuilder.lifecycleConfig(),
                        configsBuilder.metricsConfig(), configsBuilder.processorConfig(),
                        configsBuilder.retrievalConfig().maxListShardsRetryAttempts(5).initialPositionInStreamExtended(InitialPositionInStreamExtended
                        		.newInitialPosition(InitialPositionInStream.TRIM_HORIZON)));
        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.setDaemon(true);
        schedulerThread.start();

    }

}
