package ir.sahab.nimbo.jimbo.parser;

import java.util.Objects;

public class Link {
    private String href;
    private String text;

    public Link(String href, String text) {
        this.href = href;
        this.text = text;
    }

    public String getHref() {
        return href;
    }

    public String getText() {
        return text;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Link link = (Link) o;
        return Objects.equals(href, link.href) &&
                Objects.equals(text, link.text);
    }

    @Override
    public int hashCode() {
        return Objects.hash(href, text);
    }
}
