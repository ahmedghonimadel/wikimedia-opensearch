package com.learning.kafkascalableapps.wikimedia;
//https://stream.wikimedia.org/v2/stream/recentchange
//https://esjewett.github.io/wm-eventsource-demo/
public class WikimediaResponse {
    private String title;
    private String extract;

    // Getters and setters

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getExtract() {
        return extract;
    }

    public void setExtract(String extract) {
        this.extract = extract;
    }
}

