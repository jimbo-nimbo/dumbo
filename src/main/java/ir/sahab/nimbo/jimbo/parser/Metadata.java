package ir.sahab.nimbo.jimbo.parser;

import java.util.Objects;

class Metadata {
    private String name;
    private String property;
    private String content;

    Metadata(String name, String property, String content) {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Metadata metadata = (Metadata) o;
        return Objects.equals(name, metadata.name) &&
                Objects.equals(property, metadata.property) &&
                Objects.equals(content, metadata.content);
    }

    @Override
    public int hashCode() {

        return Objects.hash(name, property, content);
    }
}
