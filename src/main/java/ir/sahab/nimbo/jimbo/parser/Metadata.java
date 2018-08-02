package ir.sahab.nimbo.jimbo.parser;

public class Metadata {
    private String name;
    private String property;
    private String content;

    public Metadata(String name, String property, String content) {
        this.name = name;
        this.property = property;
        this.content = content;
    }

    public String getName() {
        return name;
    }

    public String getProperty() {
        return property;
    }

    public String getContent() {
        return content;
    }
}
