package ir.sahab.nimbo.jimbo.parser;

import java.net.URL;

public class Link {
    private URL href;
    private String text;

    public Link(URL href, String text) {
        this.href = href;
        this.text = text;
    }

    public URL getHref() {
        return href;
    }

    public String getText() {
        return text;
    }
}
