package com.kinesis.consumer.config;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.kinesis.consumer.processor.RecordProcessorFactory;
import com.kinesis.consumer.repository.UserRepository;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;


@Component
public class ConsumerConfig {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerConfig.class);

    @Value(value = "${aws.stream-name}")
    private String streamName;

    @Value(value = "${application.name}")
    private String applicationName;

    @Value(value = "${aws.region}")
    private String awsRegion;

    @Value(value = "${aws.access-key}")
    private String accessKey;

    @Value(value = "${aws.access-secret}")
    private String secretKey;

    public ConfigsBuilder getConfigBuilder(UserRepository userRepository) {
        logger.info("Getting client configucation");

        AwsCredentials awsCreds = AwsBasicCredentials.create(accessKey, secretKey);

        AwsCredentialsProvider awsCredential = StaticCredentialsProvider.create(awsCreds);

        Region region = Region.of(awsRegion);

        KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(
                KinesisAsyncClient.builder().credentialsProvider(awsCredential).region(region));

        DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(region).build();

        CloudWatchAsyncClient cloudWatchClient =
                CloudWatchAsyncClient.builder().region(region).build();

        return new ConfigsBuilder(streamName, applicationName, kinesisClient, dynamoClient,
                cloudWatchClient, UUID.randomUUID().toString(), new RecordProcessorFactory(userRepository));

    }

}