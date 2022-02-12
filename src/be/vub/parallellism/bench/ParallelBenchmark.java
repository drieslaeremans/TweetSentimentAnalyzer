package be.vub.parallellism.bench;

import be.vub.parallellism.data.models.Pair;
import be.vub.parallellism.data.models.Tweet;
import be.vub.parallellism.data.readers.TweetReader;
import be.vub.parallellism.data.readers.WordListReader;
import be.vub.parallellism.solutions.tasks.CalculateSentimentScoreTask;
import be.vub.parallellism.solutions.tasks.CountHashtagConcurrentTask;
import be.vub.parallellism.solutions.tasks.CountHashtagTask;
import be.vub.parallellism.solutions.tasks.SortSentimentsTask;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class ParallelBenchmark {
    // One ForkJoinPool for benchmarks.
    private static ForkJoinPool pool;

    // Standard list of cores to run benchmarks on if there are no command line arguments.
    public static List<Integer> preset_cores = Arrays.asList(8);
    // Sequential thresholds to be benchmarked.
    public static List<Integer> sequential_thresholds = Arrays.asList(1000, 2500, 3500, 5000, 6500, 7500, 9000, 10_000, 15_000);
    // Amount of repetition to complete on each benchmark.
    public static int n_repetitions;

    static Tweet[] tweets = new Tweet[0];
    // Half the list of tweets to benchmark Sentiment and sort operation on firefly.
    static Tweet[] halfTweets = new Tweet[0];
    static HashSet<String> positiveWords = new HashSet<>();
    static HashSet<String> negativeWords = new HashSet<>();

    public static void main(String[] args) {
        System.out.println("Starting...");
        if (args.length < 1) {
            System.out.println("Please provide '# repetitions to perform' as commandline argument.");
            return;
        }
        InitParams(args);
        LoadData();

        benchmarkLoop(
                new Pair<>("countHashtagsParallelTaskFJ", ParallelBenchmark::countHashtagsParallelTaskFJ)
                ,new Pair<>("countHashtagsConcurrentTaskFJ", ParallelBenchmark::countHashtagsConcurrentTaskFJ)
                ,new Pair<>("calculateSentimentAndSortTaskFJ", ParallelBenchmark::calculateSentimentAndSortTaskFJ)
        );
    }


    @SafeVarargs
    private static void benchmarkLoop(Pair<String, Function<Integer, Function<Integer, Object>>>... benchmarks)  {
        // Loop over every benchmark lambda.
        for(int bm_i = 0; bm_i < benchmarks.length; bm_i++) {
            System.out.println("Benchmarking " + benchmarks[bm_i].getKey());
            //Benchmark the lambda for every core.
            for (int amount_cores : preset_cores) {
                File res_file = new File("./results/runtimes_"+benchmarks[bm_i].getKey()+"_cores-"+amount_cores+".csv");
                if(res_file.exists()){
                    System.out.println(res_file+" already exists, skipping core amount "+amount_cores);
                    continue;
                }

                System.out.println("Running benchmark on " + amount_cores + " cores");
                var benchmarkWrapper = benchmarks[bm_i].getValue().apply(amount_cores);
                // Start benchmark for every sequential threshold.
                for(int st_i = 0; st_i < sequential_thresholds.size(); st_i++) {
                    System.out.print("Running benchmark with sequential threshold  " + sequential_thresholds.get(st_i) + ": ");
                    System.out.flush();
                    System.gc();
                    List<Long> rts = benchmark(benchmarkWrapper, sequential_thresholds.get(st_i));
                    write2file(res_file, sequential_thresholds.get(st_i), rts);
                    System.out.println(" v");
                }
                System.gc();
            }
        }
    }

    private static List<Long> benchmark(Function<Integer, Object> benchmark, int threshold) {
        List<Long> runtimes = new ArrayList<>(n_repetitions);
        for(int i = 0; i < n_repetitions; i++){
            System.gc(); //a heuristic to avoid Garbage Collection (GC) to take place in timed portion of the code
            long before = System.currentTimeMillis(); //time is measured in ms
            benchmark.apply(threshold);
            runtimes.add(System.currentTimeMillis()-before);
            System.out.print("|");
        }
        return runtimes;
    }

    //writes runtimes for a given strategy to file
    private static void write2file(File f, int threshold, List<Long> runtimes){
        PrintWriter csv_writer;
        try {
            csv_writer = new PrintWriter(new FileOutputStream(f,true));
            StringBuilder line = new StringBuilder().append(threshold); //StringBuilder for performance.
            for(Long rt : runtimes){
                line.append(",").append(rt);
            }
            csv_writer.println(line);
            csv_writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static void InitParams(String[] args) {
        System.out.println("Initializing params... ");
        n_repetitions = Integer.parseInt(args[0]);
        System.out.println("Repeating each measurement "+n_repetitions+" times.");

        if(args.length > 1){
            preset_cores = new ArrayList<>(args.length-1);
            for(int i = 1; i < args.length; i++){
                preset_cores.add(Integer.parseInt(args[i]));
            }
        }
    }

    private static void LoadData() {
        System.out.println("Loading Tweets & WordSets... ");
        try {
            tweets =
//                    TweetReader.readData("/data/PD/Twitter/tweets.csv").toArray(new Tweet[0]);
//                    TweetReader.readData("./files/tweets_10000.csv").toArray(new Tweet[0]);
                    TweetReader.readData("./files/tweets_3500000.csv").toArray(new Tweet[0]);
            positiveWords =
                    WordListReader.read("./files/positive-words.txt");
//                    WordListReader.read("/data/PD/Twitter/positive-words.txt");
            negativeWords =
                    WordListReader.read("./files/negative-words.txt");
//                    WordListReader.read("/data/PD/Twitter/negative-words.txt");

            halfTweets = Arrays.copyOfRange(tweets, 0, (tweets.length/2));
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("# Tweets in dataset: " + tweets.length);
        System.out.println();
    }

    /**
     * Benchmark lambda for counting hashtags.
     * @param cores amount of cores to use in ForkJoinPool
     * @return HashMap with count of hashtags.
     */
    static Function<Integer, Object> countHashtagsParallelTaskFJ(int cores) {
        pool = new ForkJoinPool(cores);
        return (Integer threshold) -> pool.invoke(new CountHashtagTask(tweets, threshold));
    }

    /**
     * Benchmark lambda for concurrently counting hashtags.
     * @param cores amount of cores to use in ForkJoinPool
     * @return ConcurrentHashMap with count of hashtags.
     */
    static Function<Integer, Object> countHashtagsConcurrentTaskFJ(int cores) {
        pool = new ForkJoinPool(cores);
        ConcurrentHashMap<String, AtomicInteger> hashtagCount = new ConcurrentHashMap<>();
        return (Integer threshold) -> {
            pool.invoke(new CountHashtagConcurrentTask(tweets, hashtagCount, threshold));
            return hashtagCount;
        };
    }

    /**
     * Benchmark lambda for sentiment calculation and sort.
     * @param cores amount of cores to use in ForkJoinPool
     * @return Array of sorted sentiment scores.
     */
    static Function<Integer, Object> calculateSentimentAndSortTaskFJ(int cores) {
        pool = new ForkJoinPool(cores);
        return (Integer threshold) -> {
            CalculateSentimentScoreTask calculateSentimentScoreTask = new CalculateSentimentScoreTask(tweets, positiveWords, negativeWords, threshold);
            pool.invoke(calculateSentimentScoreTask);
            Pair<Tweet, Integer>[] scores = calculateSentimentScoreTask.getScores();
            pool.invoke(new SortSentimentsTask(scores));
            return scores;
        };
    }
}
