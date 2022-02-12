package be.vub.parallellism.solutions.tasks;

import be.vub.parallellism.data.models.Tweet;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class to count how often hashtags are used in an array of Tweet-objects.
 * Fase 1 - Implementation 2
 *
 * @author drlaerem
 *
 * Implementation using ForkJoin-framework with a RecursiveAction.
 * Each thread contains a reference to the same ConcurrentHashMap.
 */
public class CountHashtagConcurrentTask extends RecursiveAction {
    static int SEQ_THRESHOLD;

    private Tweet[] tweets;
    private int lo;
    private int hi;
    // The use of an AtomicInteger instead of an Integer speeds up incrementing the count for a hashtag because it is mutable and concurrent by default.
    private ConcurrentHashMap<String, AtomicInteger> hashtagCount;

    /**
     * Constructor to initialize with tweets, the ConcurrentHashMap and sequential threshold.
     * @param tweets Array of Tweet-objects used to count hashtags.
     * @param hashtagCount Reference to ConcurrentHashMap shared by all threads.
     * @param threshold Sequential threshold : lower boundary to start counting hashtags instead of dividing work.
     */
    public CountHashtagConcurrentTask(Tweet[] tweets, ConcurrentHashMap<String, AtomicInteger> hashtagCount, int threshold) {
        this(tweets, 0, tweets.length, hashtagCount);
        SEQ_THRESHOLD = threshold;
    }

    /**
     * Constructor to initialize with tweets and the ConcurrentHashMap.
     * @param tweets Array of Tweet-objects used to count hashtags.
     * @param hashtagCount Reference to ConcurrentHashMap shared by all threads.
     */
    public CountHashtagConcurrentTask(Tweet[] tweets, ConcurrentHashMap<String, AtomicInteger> hashtagCount) {
        this(tweets, 0, tweets.length, hashtagCount);
        SEQ_THRESHOLD = 5000;
    }

    /**
     * Private constructor to initialize work.
     * @param tweets Array of Tweet-objects used to count hashtags.
     * @param lo Lower boundary of work for this thread.
     * @param hi Upper boundary of work for this thread.
     * @param hashtagCount Reference to ConcurrentHashMap shared by all threads.
     */
    private CountHashtagConcurrentTask(Tweet[] tweets, int lo, int hi, ConcurrentHashMap<String, AtomicInteger> hashtagCount) {
        this.tweets = tweets;
        this.lo = lo;
        this.hi = hi;
        this.hashtagCount = hashtagCount;
    }

    /**
     * Override of RecursiveAction method compute. Gets called by ForkJoinPool method invoke to run the thread after its creation.
     */
    @Override
    protected void compute() {
        // If amount of work (upper boundary - lower boundary) is lower than Sequential Threshold, perform sequential hashtag count.
        if ((hi - lo) <= SEQ_THRESHOLD) {
            for (int i = lo; i < hi; i++) {
                for (String s : tweets[i].getHashtags()) {
                    countHashtags(s);
                }
            }
        }
        // Else divide work and run threads.
        else {
            CountHashtagConcurrentTask left = new CountHashtagConcurrentTask(tweets, lo, (hi+lo)/2, hashtagCount);
            CountHashtagConcurrentTask right = new CountHashtagConcurrentTask(tweets, (hi+lo)/2, hi, hashtagCount);

            left.fork(); // Fork 1 Task first to start thread in background.
            right.compute(); // Run other Task in current thread.
            left.join(); // Call join() on forked Task to wait for it to finish.
        }
    }

    /**
     * Method to add update the ConcurrentHashMap with a count for a hashtag.
     * @param hashtag hashtag to add
     */
    private void countHashtags(String hashtag) {
        AtomicInteger value; //AtomicInteger reference.
        // putIfAbsent initializes a new key hashtag with a new AtomicInteger with value of 1,
        // if the hashtag is already in the HashMap it returns the existing value.
        // Store the existing value in reference value and call concurrent operation incrementAndGet() to increment count.
        if ((value = hashtagCount.putIfAbsent(hashtag, new AtomicInteger(1))) != null) {
            value.incrementAndGet();
        }
    }
}
