package be.vub.parallellism.data.models;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;

public class Tweet {
    private String tweetid;
    private String userid;
    private String userDisplayName;
    private String userScreenName;
    private String accountLanguage;
    private String tweetLanguage;
    private String tweetText;
    private String tweetTime;
    private List<String> hashtags;

    public String getTweetid() {
        return tweetid;
    }

    public void setTweetid(String tweetid) {
        this.tweetid = tweetid;
    }

    public String getUserid() {
        return userid;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public String getUserDisplayName() {
        return userDisplayName;
    }

    public void setUserDisplayName(String userDisplayName) {
        this.userDisplayName = userDisplayName;
    }

    public String getUserScreenName() {
        return userScreenName;
    }

    public void setUserScreenName(String userScreenName) {
        this.userScreenName = userScreenName;
    }

    public String getAccountLanguage() {
        return accountLanguage;
    }

    public void setAccountLanguage(String accountLanguage) {
        this.accountLanguage = accountLanguage;
    }

    public String getTweetLanguage() {
        return tweetLanguage;
    }

    public void setTweetLanguage(String tweetLanguage) {
        this.tweetLanguage = tweetLanguage;
    }

    public String getTweetText() {
        return tweetText;
    }

    public void setTweetText(String tweetText) {
        this.tweetText = tweetText;
    }

    public String getTweetTime() {
        return tweetTime;
    }

    public void setTweetTime(String tweetTime) {
        this.tweetTime = tweetTime;
    }

    public List<String> getHashtags() {
        return hashtags;
    }

    public void setHashtags(List<String> hashtags) {
        this.hashtags = hashtags;
    }

    @Override
    public String toString() {
        return "Tweet{" +
                "tweetid='" + tweetid + '\'' +
                ", userid='" + userid + '\'' +
                ", userDisplayName='" + userDisplayName + '\'' +
                ", userScreenName='" + userScreenName + '\'' +
                ", tweetLanguage='" + tweetLanguage + '\'' +
                ", tweetText='" + tweetText + '\'' +
                ", tweetTime='" + tweetTime + '\'' +
                ", hashtags=" + hashtags +
                '}';
    }

    public int calculateSentimentScore(HashSet<String> positiveWordList, HashSet<String> negativeWordList) throws IOException {
        String[] words = this.getTweetText().toLowerCase().split("\\P{L}+");

        int score = 0;
        for (String word : words)
            if (positiveWordList.contains(word))
                score++;
            else if (negativeWordList.contains(word))
                score--;
        return score;
    }
}