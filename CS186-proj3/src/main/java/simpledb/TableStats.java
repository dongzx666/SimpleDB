package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int tableid;
    private int ioCostPerPage;

    private int ntups;
    private HashMap<Integer, Object> histograms;
    private HashMap<Integer, Integer[]> max_min_map;

    private static final int BUCKET_NUM = 10;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the DbFile for the table in question,
        // then scan through its tuples and calculate the values that you need.
        // You should try to do this reasonably efficiently,
        // but you don't necessarily have to (for example) do everything in a single scan of the table.
        // some code goes here
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;

        this.histograms = new HashMap<>();
        this.max_min_map = new HashMap<>();
        this.ntups = 0;

        this.init();
    }

    private void init () {
        DbIterator iterator = new SeqScan(new TransactionId(), this.tableid);

        try {
            // 第一次找max和min
            iterator.open();
            while (iterator.hasNext()) {
                this.ntups++;
                Tuple tuple = iterator.next();
                TupleDesc tupleDesc = tuple.getTupleDesc();
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    // fix bug(2020-3-29) 不要用switch，会有string情况走到default
                    if (tuple.getField(i).getType() == Type.INT_TYPE) {
                        IntField field = (IntField)tuple.getField(i);
                        int val = field.getValue();
                        if (max_min_map.containsKey(i)) {
                            Integer[] max_min = max_min_map.get(i);
                            if (val > max_min[0]) max_min[0] = val;
                            if (val < max_min[1]) max_min[1] = val;
                        } else {
                            Integer[] max_min = new Integer[]{val, val};
                            max_min_map.put(i, max_min);
                        }
                    }
                }
            }
            // 第二次构建直方图
            iterator.rewind();
            while (iterator.hasNext()) {
                Tuple tuple = iterator.next();
                TupleDesc tupleDesc = tuple.getTupleDesc();
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    switch (tuple.getField(i).getType()) {
                        case INT_TYPE:
                            IntField ifield = (IntField)tuple.getField(i);
                            int ival = ifield.getValue();

                            if (histograms.containsKey(i)) {
                                IntHistogram ihis = (IntHistogram)histograms.get(i);
                                ihis.addValue(ival);
                            } else {
                                int max = max_min_map.get(i)[0];
                                int min = max_min_map.get(i)[1];
                                histograms.put(i, new IntHistogram(BUCKET_NUM, min, max));
                            }
                            break;
                        case STRING_TYPE:
                            StringField sfield = (StringField)tuple.getField(i);
                            String sval = sfield.getValue();

                            if (histograms.containsKey(i)) {
                                StringHistogram shis = (StringHistogram)histograms.get(i);
                                shis.addValue(sval);
                            } else {
                                histograms.put(i, new StringHistogram(BUCKET_NUM));
                            }
                            break;
                        default:
                            throw new IllegalStateException("can't reach here");
                    }
                }
            }

            iterator.close();
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } finally {
            iterator.close();
        }
    }



    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        HeapFile table = (HeapFile) Database.getCatalog().getDbFile(this.tableid);
        return table.numPages() * this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    // cardinality(基数), 等于总行数*选择性
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int)Math.ceil(totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        double selectivity = 0.0;
        switch (constant.getType()) {
            case INT_TYPE:
                int ival = ((IntField)constant).getValue();
                // 拿出这个字段对应的直方图
                IntHistogram ihis = (IntHistogram) histograms.get(field);
                selectivity = ihis.estimateSelectivity(op, ival);
                break;
            case STRING_TYPE:
                String sval = ((StringField)constant).getValue();
                StringHistogram shis = (StringHistogram)histograms.get(field);
                selectivity = shis.estimateSelectivity(op, sval);
                break;
            default:
                throw new IllegalStateException("can't reach here");
        }
        return selectivity;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.ntups;
    }

}
