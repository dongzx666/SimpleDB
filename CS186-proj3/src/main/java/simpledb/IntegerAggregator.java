package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */

// 用于Int型字段的聚合
// 1、当聚集函数和非聚集函数出现在一起时，需要将非聚集函数进行group by
// 2、当只做聚集函数查询时候，就不需要进行分组了。(相当于对这个字段的整列做聚合)
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private Op what;

    private TupleDesc tupleDesc;
    private List<Tuple> tuples;
    // 这里区分有没有group-by的情况，
    // 有则定义一个存放最终结果的map, 一个存放所有中间值数组的map
    private HashMap<Field, Integer> map;
    private HashMap<Field, List<Integer>> map_agg;
    // 没则直接定义一个数组, ? 但是单测是否没有测试这种情况
    private List<Integer> list_no_group;

    /**
     * Aggregate constructor
     *
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping(分组依据字段的索引)
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping(分组字段的类型)
     * @param afield
     *            the 0-based index of the aggregate field in the tuple(聚合字段的索引)
     * @param what
     *            the aggregation operator(聚合运算符)
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        // 这个好像没用用到？
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.map = new HashMap<Field, Integer>();
        this.map_agg = new HashMap<Field, List<Integer>>();
        this.list_no_group = new ArrayList<Integer>();

        this.tupleDesc = null;
        this.tuples = new ArrayList<Tuple>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        if (tup.getField(this.afield).getType() != Type.INT_TYPE) {
            throw new IllegalArgumentException("must be INT_TYPE");
        }
        if (this.tupleDesc == null) {
            this.tupleDesc = this.getTupleDesc(tup);
        }


        // 确定聚合字段
        final IntField tempA = (IntField) tup.getField(this.afield);
//        if (tempA.getType() != Type.INT_TYPE) throw new IllegalArgumentException("must be INT_TYPE");

        // 确定group-by 字段
        if (this.gbfield != Aggregator.NO_GROUPING) {
            //  Each tuple in the result is a pair of the form (groupValue, aggregateValue),
            Field tempGb = tup.getField(this.gbfield);
            if (this.map_agg.containsKey(tempGb)) {
                this.map_agg.get(tempGb).add(tempA.getValue());
            } else {
                // this.map_agg.put(tempGb, new ArrayList<Integer>(tempA.getValue()));
                this.map_agg.put(tempGb, new ArrayList<Integer>(){{add(tempA.getValue());}});
            }
        } else {
            // unless the value of the group by field was Aggregator.NO_GROUPING, in which case the result is a single tuple of the form (aggregateValue).
            this.list_no_group.add(tempA.getValue());
        }

    }

    // 依据聚合条件创建tuple模型
    private TupleDesc getTupleDesc (Tuple tp) {
        TupleDesc temp = tp.getTupleDesc();

        Type[] typrAr;
        String[] fieldAr;

        if (this.gbfield != Aggregator.NO_GROUPING) {
            typrAr = new Type[2];
            fieldAr = new String[2];
            typrAr[0] = tp.getTupleDesc().getFieldType(this.gbfield);
            fieldAr[0] = tp.getTupleDesc().getFieldName(this.gbfield);
            typrAr[1] = Type.INT_TYPE;
            fieldAr[1] = tp.getTupleDesc().getFieldName(this.afield);
        } else {
            typrAr = new Type[1];
            fieldAr = new String[1];
            typrAr[0] = Type.INT_TYPE;
            fieldAr[0] = tp.getTupleDesc().getFieldName(this.afield);
        }

        return new TupleDesc(typrAr, fieldAr);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        // throw new UnsupportedOperationException("please implement me for proj2");

        if (this.gbfield != Aggregator.NO_GROUPING) {
            switch (this.what) {
                case AVG:
                    for (Map.Entry<Field, List<Integer>> entry: map_agg.entrySet()) {
                        // fix bug sum 定义位置写在for外了，导致出现了sum错误的复用情况
                        int sum = 0;
                        for (Integer i : entry.getValue()) {
                            sum += i;
                        }
                        map.put(entry.getKey(), sum / entry.getValue().size());
                    }
                    break;
                case MIN:
                    for (Map.Entry<Field, List<Integer>> entry: map_agg.entrySet()) {
                        int min = Integer.MAX_VALUE;
                        for (Integer i : entry.getValue()) {
                            if (min > i) min = i;
                        }
                        map.put(entry.getKey(), min);
                    }
                    break;
                case MAX:
                    for (Map.Entry<Field, List<Integer>> entry: map_agg.entrySet()) {
                        int max = Integer.MIN_VALUE;
                        for (Integer i : entry.getValue()) {
                            if (max < i) max = i;
                        }
                        map.put(entry.getKey(), max);
                    }
                    break;
                case SUM:
                    for (Map.Entry<Field, List<Integer>> entry: map_agg.entrySet()) {
                        int total = 0;
                        for (Integer i : entry.getValue()) {
                            total += i;
                        }
                        map.put(entry.getKey(), total);
                    }
                    break;
                case COUNT:
                    for (Map.Entry<Field, List<Integer>> entry: map_agg.entrySet()) {
                        map.put(entry.getKey(), entry.getValue().size());
                    }
                    break;
                default:
            }
            for (Map.Entry<Field, Integer> entry: map.entrySet()) {
                Tuple temp = new Tuple(this.tupleDesc);
                temp.setField(0, entry.getKey());
                temp.setField(1, new IntField(entry.getValue()));
                this.tuples.add(temp);
            }
        } else {
            Tuple temp = new Tuple(this.tupleDesc);
            switch (this.what) {
                case AVG:
                    int sum = 0;
                    for (Integer i : this.list_no_group) {
                        sum += i;
                    }
                    temp.setField(0, new IntField(sum/this.list_no_group.size()));
                    break;
                case MIN:
                    int min = Integer.MAX_VALUE;
                    for (Integer i : this.list_no_group) {
                        if (min > i) min = i;
                    }
                    temp.setField(0, new IntField(min));
                    break;
                case MAX:
                    int max = Integer.MIN_VALUE;
                    for (Integer i : this.list_no_group) {
                        if (max < i) max = i;
                    }
                    temp.setField(0, new IntField(max));
                    break;
                case SUM:
                    int total = 0;
                    for (Integer i : this.list_no_group) {
                        total += i;
                    }
                    temp.setField(0, new IntField(total));
                    break;
                case COUNT:
                    temp.setField(0, new IntField(this.list_no_group.size()));
                    break;
                default:
                    break;
            }
            this.tuples.add(temp);
        }

        return new TupleIterator(this.tupleDesc, this.tuples);
    }

}
