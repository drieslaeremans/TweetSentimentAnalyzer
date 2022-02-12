package be.vub.parallellism.solutions.tasks;

import be.vub.parallellism.data.models.Tweet;

import java.util.HashMap;
import java.util.concurrent.RecursiveTask;

/**
 * Class to count how often hashtags are used in an array of Tweet-objects.
 * Fase 1 - Implementation 1
 *
 * @author drlaerem
 *
 * Implementation using ForkJoin-framework with a RecursiveTask.
 * Each thread created returns a HashMap that can be combined with the HashMap from other thread.
 */
public class CountHashtagTask extends RecursiveTask<HashMap<String, Integer>> {
    //Sequential threshold : lower boundary to start counting hashtags instead of creating new threads.
    static int SEQ_THRESHOLD;

    private Tweet[] tweets;
    private int lo;
    private int hi;

    /**
     * Constructor to initialize with tweets and sequential threshold.
     * @param tweets Array of Tweet-objects used to count hashtags.
     * @param threshold Sequential threshold : lower boundary to start counting hashtags instead of dividing work.
     */
    public CountHashtagTask(Tweet[] tweets, int threshold) {
        this(tweets, 0, tweets.length);
        SEQ_THRESHOLD = threshold;
    }

    /**
     * Constructor to use default threshold.
     * @param tweets Array of Tweet-objects used to count hashtags.
     */
    public CountHashtagTask(Tweet[] tweets) {
        this(tweets, 0, tweets.length);
        SEQ_THRESHOLD = 3500;
    }

    /**
     * Private constructor to initialize actual work.
     * @param tweets Array of Tweet-objects used to count hashtags.
     * @param lo Lower boundary of work for this thread.
     * @param hi Upper boundary of work for this thread.
     */
    private CountHashtagTask(Tweet[] tweets, int lo, int hi) {
        this.tweets = tweets;
        this.lo = lo;
        this.hi = hi;
    }

    /**
     * Override of RecursiveTask method compute. Gets called by ForkJoinPool method invoke to run the thread after its creation.
     * @return HashMap<String, Integer> where string is the hashtag and Integer is the amount of time it has been counted in the current subarray.
     */
    @Override
    protected HashMap<String, Integer> compute() {
        // If amount of work (upper boundary - lower boundary) is lower than Sequential Threshold, perform sequential hashtag count.
        if ((hi - lo) <= SEQ_THRESHOLD)
            return countHashtags();

        // Else divide work and run threads.
        return calculateRecursiveTasks();
    }

    /**
     * Divide work in half and run threads.
     * @return counted hashtags
     */
    private HashMap<String, Integer> calculateRecursiveTasks() {
        CountHashtagTask left = new CountHashtagTask(tweets, lo, (hi+lo)/2);
        CountHashtagTask right = new CountHashtagTask(tweets, (hi+lo)/2, hi);

        // Fork 1 Task first to start thread in background.
        left.fork();
        // Run other Task in current thread.
        HashMap<String, Integer> rightMap = right.compute();
        // Call join() on forked Task to wait for the result of the thread.
        // Combine both HashMaps with merge-operation and return it.
        left.join().forEach((key, value) -> rightMap.merge(key, value, Integer::sum));
        return rightMap;
    }

    /**
     * Sequentially count hashtags and store in HashMap.
     * @return counted hashtags
     */
    private HashMap<String, Integer> countHashtags() {
        HashMap<String, Integer> hashtagCount = new HashMap<>();
        // Loop from lower to upper boundary. -> This threads work.
        for (int i = lo; i < hi; i++) {
            // Count the hashtags for a tweet and add each of them to the HashMap using merge-operation.
            // If the hashtag already exists the merge operation will combine the old and new value with Integer::sum.
            tweets[i].getHashtags().forEach((String hashtag) -> hashtagCount.merge(hashtag, 1, Integer::sum));
        }

        // Return counted hashtags.
        return hashtagCount;
    }
}
