package ir.sahab.nimbo.jimbo;

import org.elasticsearch.client.transport.TransportClient;

import java.net.UnknownHostException;

public class Main {
    public static void main(String[] args) throws UnknownHostException {
        TransportClient client = ElasticClientBuilder.build();



        client.close();
    }
}
