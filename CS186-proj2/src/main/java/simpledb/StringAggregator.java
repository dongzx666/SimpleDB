package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */

// 注意下面的注释提到了String类型的聚合只支持count
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldType;
    private int afield;
    private Op what;

    private TupleDesc tupleDesc;
    private List<Tuple> tuples;

    private HashMap<Field, Integer> map;
    private int no_group_with_count;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) throw new IllegalArgumentException();
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.tupleDesc = null;
        this.tuples = new ArrayList<Tuple>();

        this.map = new HashMap<Field, Integer>();
        this.no_group_with_count = 0;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        if (tup.getField(this.afield).getType() != Type.STRING_TYPE) {
            throw new IllegalArgumentException("must be STRING_TYPE");
        }
        if (this.tupleDesc == null) {
            this.tupleDesc = this.getTupleDesc(tup);
        }


        Field tempA = tup.getField(this.afield);
        if (this.gbfield != Aggregator.NO_GROUPING) {
            Field tempGb = tup.getField(this.gbfield);
            if (this.map.containsKey(tempGb)) {
                this.map.put(tempGb, this.map.get(tempGb)+1);
            } else {
                this.map.put(tempGb, 1);
            }
        } else {
            this.no_group_with_count++;
        }

    }

    // 依据聚合条件创建tuple模型
    // TODO 处理重复代码
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
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        // throw new UnsupportedOperationException("please implement me for proj2");
        if (this.gbfield != Aggregator.NO_GROUPING) {
            for (Map.Entry<Field, Integer> entry: map.entrySet()) {
                Tuple temp = new Tuple(this.tupleDesc);
                temp.setField(0, entry.getKey());
                temp.setField(1, new IntField(entry.getValue()));
                this.tuples.add(temp);
            }
        } else {
            Tuple temp = new Tuple(this.tupleDesc);
            temp.setField(0, new IntField(this.no_group_with_count));
            this.tuples.add(temp);
        }

        return new TupleIterator(this.tupleDesc, this.tuples);
    }

}
