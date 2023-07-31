package io.pixelsdb.pixels.planner.plan.logical.operation;

public class ListNode {
    
    ListNode next;

    private String name;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ListNode getNext() {
        return next;
    }

    public void setNext(ListNode next) {
        this.next = next;
    }

    public ListNode(String name) {
        this.name = name;
    }

    public ListNode() {
        this.name = null;
    }

    public void printData() {
        System.out.println("ListNode");
    }
}
