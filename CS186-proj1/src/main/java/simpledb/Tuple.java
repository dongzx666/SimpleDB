package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
// 元组相当于表中的一行，由多个Field组成
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

//    private List<Field> Fields;
    private Field[] Fields;

    private TupleDesc tupleDesc;

    private RecordId recordId;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.tupleDesc = td;
        this.Fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if (i < 0 || i >= this.Fields.length) {
            throw new NoSuchElementException ("i is invalid");
        }
        this.Fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if (i < 0 || i >= this.Fields.length) {
            throw new NoSuchElementException ("i is invalid");
        }
        return this.Fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     *
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // some code goes here
        // throw new UnsupportedOperationException("Implement this");
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < this.Fields.length; i++) {
            stringBuffer.append(this.Fields[i].toString());
            if (i != this.Fields.length-1) {
                stringBuffer.append("\t");
            } else {
                stringBuffer.append("\n");
            }
        }
        return stringBuffer.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the Fields of this tuple
     * */
    public Iterator<Field> Fields()
    {
        // some code goes here
        if (this.Fields.length == 0) throw new NoSuchElementException("Fields is empty");
        return new Iterator<Field>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                return index < Fields.length;
            }

            @Override
            public Field next() {
                if (!this.hasNext()) throw new NoSuchElementException("has not next");
                return Fields[index++];
            }
        };
    }
}
