package ir.sahab.nimbo.jimbo.main;

class NewsSite {
    private String url;
    private String tag;
    private String attribute;
    private String value;
    String getTag() {
        return tag;
    }

    String getAttribute() {
        return attribute;
    }

    String getAttributeValue() {
        return value;
    }

    String getUrl() {
        return url;
    }

    NewsSite(String url, String tag, String attribute, String value) {
        this.url = url;
        this.tag = tag;
        this.attribute = attribute;
        this.value = value;
    }
}
