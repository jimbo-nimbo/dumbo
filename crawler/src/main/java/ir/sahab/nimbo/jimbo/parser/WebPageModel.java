package ir.sahab.nimbo.jimbo.parser;

public class WebPageModel {

    private final String html;
    private final String link;

    public WebPageModel(String html, String link) {
        this.html = html;
        this.link = link;
    }

    public String getHtml() {
        return html;
    }

    public String getLink() {
        return link;
    }
}
