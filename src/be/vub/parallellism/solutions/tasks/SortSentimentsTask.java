package be.vub.parallellism.solutions.tasks;

import be.vub.parallellism.data.models.Pair;
import be.vub.parallellism.data.models.Tweet;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.RecursiveAction;

/**
 * Class to Sort an array of Tweet sentiments based on sentiment value.
 * Fase 2
 *
 * @author drlaerem
 *
 * Implementation using ForkJoin-Framework with a RecursiveAction.
 * The input array will be sorted after invoking the action.
 */
public class SortSentimentsTask extends RecursiveAction {
    private Pair<Tweet, Integer>[] array;
    private int lo;
    private int hi;

    /**
     * Public constructor to initialize action.
     * @param array
     */
    public SortSentimentsTask(Pair<Tweet, Integer>[] array) {
        this(array, 0, array.length);
    }

    /**
     * Private constructor to devide work.
     * @param array
     * @param lo
     * @param hi
     */
    private SortSentimentsTask(Pair<Tweet, Integer>[] array, int lo, int hi) {
        this.array = array;
        this.lo = lo;
        this.hi = hi;
    }

    /**
     * Override of RecursiveAction method compute. Gets called by ForkJoinPool method invoke to run the thread after its creation.
     */
    @Override
    protected void compute() {
        // If single element is reached, return. Merging starts here.
        if (lo >= (hi - 1)) return;

        // Else divide work in half.
        int middle = lo + (hi-lo)/2;
        SortSentimentsTask left = new SortSentimentsTask(array, lo, middle);
        SortSentimentsTask right = new SortSentimentsTask(array, middle, hi);

        //Fork one action and compute the other in this thread. Call join on forked thread to wait on result.
        left.fork(); right.compute(); left.join();
        merge(middle); //Merge results from previous calls.
    }

    /**
     * Merge the subarrays lo to middle and middle to hi.
     * @param middle Midpoint of the work of this action.
     */
    private void merge(int middle) {
        Pair<Tweet, Integer>[] workingArray = new Pair[(hi - lo)]; // Init new workingArray the size of current work.
        int leftI = lo; //Init iterators.
        int rightI = middle; //Init iterators.

        // Loop through length of workingArray.
        for (int i = 0; i < workingArray.length; i++) {
            //If end in both subarrays has not been reached.
            if (leftI < middle && rightI < hi) {
                // Copy largest current element to workingArray and increment used iterator.
                if (array[leftI].getValue() <= array[rightI].getValue()) {
                    workingArray[i] = array[leftI++];
                } else {
                    workingArray[i] = array[rightI++];
                }
            }
            //Else flush the element of the array that needs to be completed to the workingArray.
            else {
                if (!(leftI < middle)) {
                    workingArray[i] = array[rightI++];
                } else {
                    workingArray[i] = array[leftI++];
                }
            }
        }
        // Copy sorted workingArray back to base array.
        for (int i = lo, j = 0; i < hi; i++) {
            array[i] = workingArray[j++];
        }
    }
}
