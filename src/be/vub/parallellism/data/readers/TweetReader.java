package be.vub.parallellism.data.readers;


import be.vub.parallellism.data.models.Tweet;
import be.vub.parallellism.data.readers.adapters.ParseStringList;
import org.supercsv.cellprocessor.ConvertNullTo;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvBeanReader;
import org.supercsv.io.ICsvBeanReader;
import org.supercsv.prefs.CsvPreference;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.function.Function;

/**
 * @author Sam van den Vonder
 */
public class TweetReader {

    static public ArrayList<Tweet> readData(String file) throws IOException {
        return readData(file, Double.POSITIVE_INFINITY, any -> true);
    }

    static public ArrayList<Tweet> readData(String file, long howMany) throws IOException {
        return readData(file, howMany, any -> true);
    }

    static public ArrayList<Tweet> readData(String file, Function<Tweet, Boolean> shouldInclude) throws IOException {
        return readData(file, Double.POSITIVE_INFINITY, shouldInclude);
    }


    /**
     * Read a file of tweets and return an in-memory list of records
     *
     * @param file Path to the data
     * @param howMany How many records do you want to read?
     * @param shouldInclude Lambda to filter csv records
     * @return A list of records representing tweets
     */
    static public ArrayList<Tweet> readData(String file, double howMany, Function<Tweet, Boolean> shouldInclude) throws IOException {
        ArrayList<Tweet> result = new ArrayList<>();

        try (ICsvBeanReader beanReader = new CsvBeanReader(new FileReader(file), CsvPreference.STANDARD_PREFERENCE)) {
            String[] columnToFieldMapping = {
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
                    new ConvertNullTo("\"\""), // tweetText
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
                    new ParseStringList(), // hashtags
                    null,
                    null,
                    null};

            String[] header = beanReader.getHeader(true);

            Tweet tweet;
            double count = 0;
            while ((tweet = beanReader.read(Tweet.class, columnToFieldMapping, processors)) != null && count < howMany) {
                if (shouldInclude.apply(tweet)) {
                    result.add(tweet);
                    count++;
                }
            }
        }
        return result;
    }
}