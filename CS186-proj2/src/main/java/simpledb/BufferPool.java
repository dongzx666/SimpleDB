package simpledb;

import java.io.*;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
// 缓冲池，通常包含常驻于内存中多张页，并且与事务的一些操作有关
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    // 用LRUCache代替Map
    // private Map<PageId, Page> pageMap;
    private final int PAGE_NUM;
    private LRUCache pageCache;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
//        this.pageMap = new HashMap<PageId, Page>(numPages);
        this.PAGE_NUM = numPages;
        this.pageCache = new LRUCache(numPages);
    }

    /**
     * Retrieve(检索) the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted(驱逐) and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException, DbException {
        // some code goes here
        // TODO Permission没处理

        Page page = this.pageCache.get(pid);

        if (page != null) {
            return page;
        } else {
            HeapFile file = (HeapFile) Database.getCatalog().getDbFile(pid.getTableId());
            Page pageRead = (HeapPage) file.readPage(pid);
            this.pageCache.set(pid, pageRead);
            return pageRead;
        }

        /*// 直接命中和非直接命中的情况
        if (pageMap.containsKey(pid)) {
            return pageMap.get(pid);
        } else {
            DbFile dbFile = Database.getCatalog().getDbFile(pid.getTableId());
            Page page = dbFile.readPage(pid);
            if (pageMap.size() < PAGE_NUM) {
                pageMap.put(pid, page);
            } else {
                // 非直接命中且bufferpool容量不够, 应该是LRU算法，这里先简单的使用RANDOM
                Random random = new Random();
                int i = random.nextInt(PAGE_NUM-1), j = 0;
                for (PageId pageId : pageMap.keySet()) {
                    if (j == i) {
                        pageMap.remove(pageId);
                        pageMap.put(pid, page);
                    } else {
                        j++;
                    }
                }
            }
            return page;
        }*/
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock
     * acquisition is not needed for lab2). May block if the lock cannot
     * be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        DbFile dbFile = Database.getCatalog().getDbFile(tableId);
        ArrayList<Page> pages = dbFile.insertTuple(tid, t);
        for (int i = 0; i < pages.size(); i++) {
            // dbFile.writePage(pages.get(i));
            pages.get(i).markDirty(true, tid);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        DbFile dbFile = Database.getCatalog().getDbFile(t.getRecordId().getPageId().getTableId());
        dbFile.deleteTuple(tid, t);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        List<PageId> pids = new ArrayList<PageId>();

        for (PageId pid : pids) {
            this.flushPage(pid);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
	// not necessary for proj1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        Page page = this.pageCache.get(pid);
        TransactionId tid = page.isDirty();
        if (tid != null) {
            DbFile file = Database.getCatalog().getDbFile(pid.getTableId());
            file.writePage(page);
            page.markDirty(false, tid);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj1
    }

}
