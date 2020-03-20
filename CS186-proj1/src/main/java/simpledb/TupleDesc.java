package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
// 用于描述元组的概形
public class TupleDesc implements Serializable {

    private List<TDItem> TDItems = new ArrayList<>();

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;

        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */


    public Iterator<TDItem> iterator() {
        // some code goes here
        if (TDItems.size() == 0) throw new NoSuchElementException("empty");
        return new Iterator<TDItem>() {
            private Integer index = 0;

            @Override
            public boolean hasNext () {
                return index < TDItems.size();
            }

            @Override
            public TDItem next () {
                if (!hasNext()) throw new NoSuchElementException("has not next");
                return TDItems.get(index++);
            }

        };
    }

    private static final long serialVersionUID = 1L;

    public TupleDesc () {}

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        if (typeAr.length == 0 || fieldAr.length == 0) {
            throw new IllegalArgumentException("不能为空");
        }
        if (typeAr.length != fieldAr.length) {
            throw new IllegalArgumentException("长度不匹配");
        }
        for (int i = 0; i < typeAr.length; i++) {
            this.TDItems.add(new TDItem(typeAr[i], fieldAr[i]));
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        String name = "unname ";
        for (int i = 0; i < typeAr.length; i++) {
            this.TDItems.add(new TDItem(typeAr[i], name+i));
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return this.TDItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= this.numFields()) {
            throw new NoSuchElementException ("i is invalid");
        }
        return this.TDItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= this.numFields()) {
            throw new NoSuchElementException ("i is invalid");
        }
        return TDItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (name == null || name.length() == 0) {
            throw new NoSuchElementException ("name can't be empty");
        }
        for (int i = 0; i < this.numFields(); i++) {
            if (this.getFieldName(i).equals(name)) return i;
        }
        throw new NoSuchElementException ("no field with a matching name is found.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (int i = 0; i < this.numFields(); i++) {
//            TODO 是否细化?
//            Type type = this.getFieldType(i);
//            if (type == Type.INT_TYPE) {
//                size += Type.INT_TYPE.getLen();
//            } else if (type == Type.STRING_TYPE) {
//                size += Type.STRING_TYPE.getLen();
//            }
            size += this.getFieldType(i).getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        TupleDesc tupleDesc = new TupleDesc();
        int len1 = td1.numFields();
        int len2 = td2.numFields();

        for (int i = 0; i < len1; i++) {
            tupleDesc.TDItems.add(td1.TDItems.get(i));
        }

        for (int i = 0; i < len2; i++) {
            tupleDesc.TDItems.add(td2.TDItems.get(i));
        }

        return tupleDesc;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
        // o == this || o is null and this is null
        if (o == this) return true;
        // this isn't null but o is null
        if (o == null) return false;
        // o is same type of object
        // getClass() != o.getClass() also can do this
        if (!(o instanceof TupleDesc)) return false;
        TupleDesc tupleDesc = (TupleDesc)o;
        // compare the length of tuple field
        if (this.numFields() != tupleDesc.numFields()) return false;
        for (int i = 0; i < this.numFields(); i++) {
            if (this.getFieldType(i) != tupleDesc.getFieldType(i)) return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        // throw new UnsupportedOperationException("unimplemented");
        if (this.TDItems.size() == 0) {
            return 0;
        }
        int res = 1;
        for (int i = 0; i < this.numFields(); i++) {
            res = 31 * res + this.getFieldName(i).hashCode();
        }
        return res;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuffer stringBuffer = new StringBuffer();
        for (int i = 0; i < this.numFields(); i++) {
            if (i != 0) stringBuffer.append(" ");
            stringBuffer.append(this.getFieldType(i).toString());
            stringBuffer.append("(");
            stringBuffer.append(this.getFieldName(i).toString());
            stringBuffer.append("),");
        }
        return stringBuffer.toString();
    }
}
