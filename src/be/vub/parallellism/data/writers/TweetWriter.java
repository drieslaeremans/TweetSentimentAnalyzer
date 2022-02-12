package be.vub.parallellism.data.writers;


import be.vub.parallellism.data.models.Tweet;
import be.vub.parallellism.data.readers.adapters.WriteStringList;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class TweetWriter {

    static public void writeToFile(String path, List<Tweet> tweets) throws IOException {
        try (ICsvBeanWriter beanWriter = new CsvBeanWriter(new FileWriter(path), CsvPreference.STANDARD_PREFERENCE)){
            // https://storage.googleapis.com/twitter-election-integrity/hashed/Twitter_Elections_Integrity_Datasets_hashed_README.txt

            String[] header = {
                    "tweetid",
                    "userid",
                    "user_display_name",
                    "user_screen_name",
                    "user_reported_location",
                    "user_profile_description",
                    "user_profile_url",
                    "follower_count",
                    "following_count",
                    "account_creation_date",
                    "account_language",
                    "tweet_language",
                    "tweet_text",
                    "tweet_time",
                    "tweet_client_name",
                    "in_reply_to_tweetid",
                    "in_reply_to_userid",
                    "quoted_tweet_tweetid",
                    "is_retweet",
                    "retweet_userid",
                    "retweet_tweetid",
                    "latitude",
                    "longitude",
                    "quote_count",
                    "reply_count",
                    "like_count",
                    "retweet_count",
                    "hashtags",
                    "urls",
                    "user_mentions",
                    "poll_choices"};

            String[] fieldToColumnMapping = {
                    "tweetid",
                    "userid",
                    "userDisplayName",
                    "userScreenName",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    "accountLanguage",
                    "tweetLanguage",
                    "tweetText",
                    "tweetTime",
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    "hashtags",
                    null,
                    null,
                    null};

            final CellProcessor[] processors = new CellProcessor[]{
                    new NotNull(), // tweetid
                    new NotNull(), // userid
                    new NotNull(), // user_display_name
                    new NotNull(), // user_screen_name
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    new Optional(), // accountLanguage
                    new Optional(), // tweetLanguage
                    new NotNull(), // tweetText
                    new NotNull(), //tweet_time
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    new WriteStringList(), // hashtags
                    null,
                    null,
                    null};
            // write the header
            beanWriter.writeHeader(header);

            // write the customer beans
            for( final Tweet tweet : tweets ) {
                beanWriter.write(tweet, fieldToColumnMapping, processors);
            }
        }
    }
}