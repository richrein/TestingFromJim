package com.fedextesting;

public class TimingResult {
    private String _wholeTerm;
    private String _shortenedTerm;
    private Long _milliseconds;

    public TimingResult() {

    }

    public TimingResult(String wholeTerm, String shortenedTerm, long milliseconds) {
        _wholeTerm = wholeTerm;
        _shortenedTerm = shortenedTerm;
        _milliseconds = milliseconds;
    }

    public String getWholeTerm(){
        return _wholeTerm;
    }

    public String getShortenedTerm(){
        return _shortenedTerm;
    }

    public long getMilliseconds(){
        return _milliseconds;
    }

    public void setWholeTerm(String wholeTerm){
        _wholeTerm = wholeTerm;
    }

    public void setShortenedTerm(String shortenedTerm){
        _shortenedTerm = shortenedTerm;
    }

    public void setMilliseconds(Long milliseconds){
        _milliseconds = milliseconds;
    }

}
