package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
// HeapFile是DbFile的一种实现形式，数据库文件包含很多页，而每个页又包含很多元组。(文件，元组描述)
public class HeapFile implements DbFile {

    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return this.f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        byte[] buf = new byte[BufferPool.PAGE_SIZE];
        Page page = null;
        try {
            InputStream in = new FileInputStream(this.f);
            // TODO offset is right?
            int offset = pid.pageNumber() * BufferPool.PAGE_SIZE;
            if (offset > 0) in.skip(offset);
            in.read(buf);
            page = new HeapPage((HeapPageId)pid, buf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)(this.f.length()/BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs

    // You will also need to implement the HeapFile.iterator() method,
    // which should iterate through through the tuples of each page in the HeapFile.
    // The iterator must use the BufferPool.getPage() method to access pages in the HeapFile.
    // This method loads the page into the buffer pool and will eventually be used
    // (in a later project) to implement locking-based concurrency control and recovery.
    // Do not load the entire table into memory on the open() call --
    // this will cause an out of memory error for very large tables.
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid, this.numPages());
    }

    private class HeapFileIterator implements DbFileIterator {
        private TransactionId tid;
        // 遍历页的索引
        private int pagePos;
        // 每文件有几页
        private int numpages;
        // 迭代器
        private Iterator<Tuple> tupleIterator;

        public HeapFileIterator(TransactionId tid, int numPages) {
            this.tid = tid;
            this.numpages = numPages;
            this.pagePos = 0;
            this.tupleIterator = null;
        }

        public Iterator<Tuple> getTuplesInPage(HeapPageId pid) throws DbException, TransactionAbortedException {
            // This method loads the page into the buffer pool and will eventually be used
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return page.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pagePos = 0;
            // Do not load the entire table into memory on the open() call
            HeapPageId pid = new HeapPageId(getId(), pagePos);
            this.tupleIterator = this.getTuplesInPage(pid);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.tupleIterator == null) return false;
            if (this.tupleIterator.hasNext()) return true;
            // TODO 是否还有其他情况
            if (this.pagePos < this.numpages - 1) {
                this.pagePos++;
                HeapPageId pid = new HeapPageId(getId(), pagePos);
                tupleIterator = getTuplesInPage(pid);
                //这时不能直接return ture，有可能返回的新的迭代器是不含有tuple的
                return tupleIterator.hasNext();
            } else return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!hasNext()) throw new NoSuchElementException();
            return this.tupleIterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.open();
        }

        @Override
        public void close() {
            pagePos = 0;
            tupleIterator = null;
        }

    }

}

