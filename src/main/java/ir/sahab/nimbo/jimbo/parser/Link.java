package ir.sahab.nimbo.jimbo.parser;

import java.net.URL;
import java.util.Objects;

public class Link {
    private URL href;
    private String text;

    Link(URL href, String text) {
        this.href = href;
        this.text = text;
    }

    URL getHref() {
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
