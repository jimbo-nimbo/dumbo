package ir.sahab.nimbo.jimbo.kafka;

import ir.sahab.nimbo.jimbo.main.Setting;

class KafkaSetting extends Setting{

    private final String bootstrapServers;
    private final String producerClientId;
    private final String consumerGroupId;
    private final int maxPollRecords;

    KafkaSetting() {
        super("kafkaConf");

        bootstrapServers = properties.getProperty("bootstrap_servers");
        producerClientId = properties.getProperty("producer_client_id");
        consumerGroupId = properties.getProperty("consumer_group_id");
        maxPollRecords = Integer.parseInt(properties.getProperty("max_poll_records"));
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getProducerClientId() {
        return producerClientId;
    }

    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

}
