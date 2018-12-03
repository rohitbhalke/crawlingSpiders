package com.cse535.lab4.model;

public class CitySentiment {

    String city;
    long positiveCount;

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public long getPositiveCount() {
        return positiveCount;
    }

    public void setPositiveCount(long positiveCount) {
        this.positiveCount = positiveCount;
    }

    public long getNegativeCount() {
        return negativeCount;
    }

    public void setNegativeCount(long negativeCount) {
        this.negativeCount = negativeCount;
    }

    public long getNeutralCount() {
        return neutralCount;
    }

    public void setNeutralCount(long neutralCount) {
        this.neutralCount = neutralCount;
    }

    long negativeCount;
    long neutralCount;
}
