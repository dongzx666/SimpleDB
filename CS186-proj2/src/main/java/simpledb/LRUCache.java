package simpledb;

import java.util.HashMap;

public class LRUCache {

    private final int CAPACITY;
    private HashMap<PageId, Node> map;
    private Node head;
    private Node end;


    public LRUCache(int CAPACITY) {
        this.CAPACITY = CAPACITY;
        this.map = new HashMap<PageId, Node>();

    }

    public Page get (PageId pid) {
        if (map.containsKey(pid)) {
            Node n = map.get(pid);
            this.remove(n);
            this.setHead(n);
            return n.page;
        }
        return null;
    }

    public void remove (Node n) {
        if (n.pre != null) {
            n.pre.next = n.next;
        } else {
            this.head = n.next;
        }

        if (n.next != null) {
            n.next.pre = n.pre;
        } else {
            this.end = n.pre;
        }
    }

    private void setHead (Node n) {
        n.next = this.head;
        n.pre = null;

        if (this.head != null) {
            this.head.pre = n;
        } else {
            this.head = n;
        }

        if (this.end == null) this.end = this.head;
    }

    public void set (PageId pid, Page page) {
        if (map.containsKey(pid)) {
            Node old = map.get(pid);
            old.page = page;
            this.remove(old);
            this.setHead(old);
        } else {
            Node created = new Node(pid, page);
            if (map.size() >= this.CAPACITY) {
                map.remove(end.pid);
                this.remove(end);
                this.setHead(created);
            } else {
                this.setHead(created);
            }
            map.put(pid, created);
        }
    }


    private class Node {
        PageId pid;
        Page page;
        Node pre;
        Node next;

        public Node(PageId pid, Page page) {
            this.pid = pid;
            this.page = page;
            this.pre = null;
            this.next = null;
        }
    }
}
