package be.vub.parallellism.solutions;


import be.vub.parallellism.data.models.Pair;
import be.vub.parallellism.data.models.Tweet;
import be.vub.parallellism.data.readers.TweetReader;
import be.vub.parallellism.data.readers.WordListReader;
import be.vub.parallellism.solutions.tasks.CalculateSentimentScoreTask;
import be.vub.parallellism.solutions.tasks.CountHashtagConcurrentTask;
import be.vub.parallellism.solutions.tasks.CountHashtagTask;
import be.vub.parallellism.solutions.tasks.SortSentimentsTask;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class SequentialAnalyser {

    /*  Code to read only English tweets from the original dataset published by Twitter
        https://about.twitter.com/en_us/values/elections-integrity.html#data

        List<Tweet> tweets = TweetReader.readData("./files/ira_tweets_csv_hashed.csv", (Tweet tweet) -> {
            String accountLang = tweet.getAccountLanguage();
            String tweetLang = tweet.getTweetLanguage();
            boolean tweetIsEnglish = tweetLang != null && (tweetLang.equals("en") || tweetLang.equals("en-gb"));
            boolean accountIsEnglish = accountLang != null && (accountLang.equals("en") || accountLang.equals("en-gb"));
            return tweetIsEnglish || (tweetLang == null && accountIsEnglish);
        });
    */

    static Integer[] thres = {1, 1000, 3500, 5000, 10000};


    public static void main(String[] args) {
        try {
//            List<Tweet> tweets = TweetReader.readData("./files/ira_tweets_csv_hashed_english.csv");
            List<Tweet> tweets = TweetReader.readData("./files/tweets_3500000.csv");
            HashSet<String> positiveWords = WordListReader.read("./files/positive-words.txt");
            HashSet<String> negativeWords = WordListReader.read("./files/negative-words.txt");
            Tweet[] tweets1 = tweets.toArray(new Tweet[0]);
            System.out.println("# Tweets in dataset: " + tweets.size());

            long before = System.currentTimeMillis();
            HashMap<String, Integer> hashtagCount = new HashMap<>();
            for (Tweet tweet : tweets) {
                List<String> hashtags = tweet.getHashtags();

                hashtags.forEach((String hashtag) -> {
                    if (hashtagCount.containsKey(hashtag)) {
                        int currentCount = hashtagCount.get(hashtag);
                        hashtagCount.put(hashtag, currentCount + 1);
                    } else
                        hashtagCount.put(hashtag, 1);
                });
            }
            long after = System.currentTimeMillis();
            System.out.println("Elapsed time in milliseconds to count all hashtags: " + (after - before));


            //Just for demonstration purposes, sort all hashtags in ascending order.
            Map<String, Integer> sortedMap = hashtagCount.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
            // System.out.println(sortedMap.toString());


            // calculate sentiment score for all tweets & sort them in ascending order
            before = System.currentTimeMillis();

            ArrayList<Pair<Tweet, Integer>> scores = new ArrayList<>(tweets.size());
            tweets.forEach((Tweet tweet) -> {
                try {
                    scores.add(new Pair<>(tweet, tweet.calculateSentimentScore(positiveWords, negativeWords)));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            scores.sort(Comparator.comparing(Pair::getValue));
            after = System.currentTimeMillis();
            System.out.println("Elapsed time in milliseconds to score and sort all tweets: " + (after - before));


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
