package ir.sahab.nimbo.jimbo.hbase;

import ir.sahab.nimbo.jimbo.parser.Link;

import java.util.List;

public class HBaseDataModel {

    private final String url;
    private final List<Link> links;

    public HBaseDataModel(String url, List<Link> links) {
        this.url = url;
        this.links = links;
    }

    public String getUrl() {
        return url;
    }

    public List<Link> getLinks() {
        return links;
    }

}
