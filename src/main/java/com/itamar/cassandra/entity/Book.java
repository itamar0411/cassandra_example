package com.itamar.cassandra.entity;

import java.util.UUID;

public class Book {

    private UUID id;

    private String title;

    private String subject;

    private String blurb;

    private String author;

    public UUID getId() {
        return id;
    }

    public String getTitle() {
        return title;
    }

    public String getSubject() {
        return subject;
    }

    public String getBlurb() {
        return blurb;
    }

    public String getAuthor() {
        return author;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public void setBlurb(String blurb) {
        this.blurb = blurb;
    }

    public void setAuthor(String author) {
        this.author = author;
    }
}
