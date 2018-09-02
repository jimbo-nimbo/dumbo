package ir.sahab.nimbo.jimbo.elasticsearch;

import ir.sahab.nimbo.jimbo.main.Setting;

public class ElasticsearchSetting extends Setting {

    private final String clusterName;
    private final String hosts;
    private final int bulkSize;
    private final String indexName;
    private final int numberOfThreads;

    public ElasticsearchSetting() {
        super("elasticsearch");
        clusterName = properties.getProperty("cluster.name");
        hosts = properties.getProperty("hosts");
        bulkSize = Integer.parseInt(properties.getProperty("bulk_size"));
        indexName = properties.getProperty("index_name");
        numberOfThreads = Integer.parseInt(properties.getProperty("elasticserach.thread.count"));
    }

    public String getIndexName() {
        return indexName;
    }

    String getHosts() {
        return hosts;
    }

    int getBulkSize() {
        return bulkSize;
    }

    public int getNumberOfThreads() {
        return numberOfThreads;
    }

    String getClusterName() {
        return clusterName;
    }
}
