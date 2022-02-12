package be.vub.parallellism.solutions.tasks;

import be.vub.parallellism.data.models.Pair;
import be.vub.parallellism.data.models.Tweet;

import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.RecursiveAction;

/**
 * Class to calculate the sentiment value of each Tweet in a Tweet-array.
 * Fase 2
 *
 * @author drlaerem
 *
 * Implementation using ForkJoin-Framework with a RecursiveAction.
 */
public class CalculateSentimentScoreTask extends RecursiveAction {
    static int SEQ_THRESHOLD;

    private Tweet[] tweets;
    private int lo;
    private int hi;
    private Pair<Tweet, Integer>[] scores;
    private HashSet<String> positiveWords;
    private HashSet<String> negativeWords;

    public CalculateSentimentScoreTask(Tweet[] tweets, HashSet<String> positiveWords, HashSet<String> negativeWords, int threshold)  {
        this(tweets, 0, tweets.length, new Pair[tweets.length], positiveWords, negativeWords);
        SEQ_THRESHOLD = threshold;
    }

    public CalculateSentimentScoreTask(Tweet[] tweets, HashSet<String> positiveWords, HashSet<String> negativeWords)  {
        this(tweets, 0, tweets.length, new Pair[tweets.length], positiveWords, negativeWords);
        SEQ_THRESHOLD = 2500;
    }

    private CalculateSentimentScoreTask(Tweet[] tweets, int lo, int hi, Pair<Tweet, Integer>[] scores,
                                       HashSet<String> positiveWords, HashSet<String> negativeWords) {
        this.tweets = tweets;
        this.lo = lo;
        this.hi = hi;
        this.scores = scores;
        this.positiveWords = positiveWords;
        this.negativeWords = negativeWords;
    }

    /**
     * Override of RecursiveAction method compute. Gets called by ForkJoinPool method invoke to run the thread after its creation.
     */
    @Override
    protected void compute() {
        // If amount of work (upper boundary - lower boundary) is lower than Sequential Threshold, calculate sentiment for current sub-array.
        if ((hi-lo) <= SEQ_THRESHOLD)
            calculateSentimentScore();
        // Else divide work.
        else
            calculateRecursiveTasks();
    }

    /**
     * Divide work in half and run threads.
     */
    private void calculateRecursiveTasks() {
        CalculateSentimentScoreTask left =
                new CalculateSentimentScoreTask(tweets, lo, (hi+lo)/2, scores, positiveWords, negativeWords);
        CalculateSentimentScoreTask right =
                new CalculateSentimentScoreTask(tweets, (hi+lo)/2, hi, scores, positiveWords, negativeWords);

        left.fork(); // Fork 1 Task first to start thread in background.
        right.compute(); // Run other Task in current thread.
        left.join(); // Call join() on forked Task to wait for it to finish.
    }

    /**
     * Sequentially calculate the sentiment of each tweet in the range and store it in the same location in the scores array.
     */
    private void calculateSentimentScore()  {
        for (int i = lo; i < hi; i++) {
            try {
                scores[i] = new Pair<>(tweets[i], tweets[i].calculateSentimentScore(positiveWords, negativeWords));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Public method to access result.
     * @return Pair<Tweet, Integer>[]
     */
    public Pair<Tweet, Integer>[] getScores() {
        return scores;
    }
}
