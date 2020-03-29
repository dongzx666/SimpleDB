package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {


    private int min;
    private int max;
    private double bucket_size;
    private int ntups;
    private int[] histogram;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.min = min;
        this.max = max;
        // 每个桶中存放的数量, 可能不是整数
        this.bucket_size = (double)(max - min + 1) / buckets;
        this.ntups = 0;
        this.histogram = new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int bucket = (int) Math.floor((v - min) / bucket_size);
        this.histogram[bucket]++;
        this.ntups++;
    }


    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        double selectivity = 0.0;

        switch (op) {
            case EQUALS:
                selectivity = estimateEquals(v);
                break;
            case NOT_EQUALS:
                selectivity = 1 - estimateEquals(v);
                break;
            case GREATER_THAN:
                selectivity = estimateGreaterThan(v);
                break;
            case GREATER_THAN_OR_EQ:
                selectivity = estimateGreaterThan(v) + estimateEquals(v);
                break;
            case LESS_THAN:
                selectivity = estimateLessThan(v);
                break;
            case LESS_THAN_OR_EQ:
                selectivity = estimateLessThan(v) + estimateEquals(v);
                break;
            default:
                throw new IllegalStateException("not reach this");
        }

        return selectivity;
    }



    private double estimateEquals (int v) {
        if (v < min || v > max) return 0.0;
        double selectivity = 0.0;
        int bucket = (int) Math.floor((v - min) / this.bucket_size);
        double h = this.histogram[bucket];
        // the selectivity of the expression is roughly (h / w) / ntups
        selectivity = h / this.bucket_size / this.ntups;
        return selectivity;
    }

    private double estimateGreaterThan (int v) {
        if (v >= max) return 0.0;
        if (v < min) return 1.0;

        double selectivity = 0.0;
        int bucket = (int) Math.floor((v - min) / this.bucket_size);
        double h_b = this.histogram[bucket];
        double b_f = h_b / this.ntups;
        // bucket是从0开始的
        double b_right = this.min + this.bucket_size * (bucket+1) - 1;
        double b_part = (b_right - v) / this.bucket_size;
        selectivity += b_f * b_part;

        // In addition, buckets b+1...NumB-1 contribute all of their selectivity
        for (int i = bucket+1; i < this.histogram.length; i++) {
            // fix bug(2020-3-28): double和int混算会出问题(隐式转化)
            // error: selectivity += this.histogram[i] / this.ntups;
            selectivity += (double)this.histogram[i] / this.ntups;
        }

        return selectivity;
    }

    private double estimateLessThan (int v) {
        if (v <= min) return 0.0;
        if (v > max) return 1.0;

        double selectivity = 0.0;
        int bucket = (int) Math.floor((v - min) / this.bucket_size);
        double h_b = this.histogram[bucket];
        double b_f = h_b / this.ntups;
        double b_left = this.min + this.bucket_size * bucket;
        double b_part = (v - b_left) / this.bucket_size;
        // fix bug(3-28) 从b_part为灰色(定义后未使用该变量)发现
        // error: selectivity += b_f * b_left;
        selectivity += b_f * b_part;

        for (int i = 0; i < bucket; i++) {
            selectivity += (double)this.histogram[i] / this.ntups;
        }

        return selectivity;
    }
    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return String.format("max is : %d, min is %d, bucket_size is %.2f, num of tuples is %d", max, min, bucket_size, ntups);
    }

}
