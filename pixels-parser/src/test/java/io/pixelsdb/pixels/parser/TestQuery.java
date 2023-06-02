package io.pixelsdb.pixels.parser;

public abstract class TestQuery {
    
    public static final String Q1 = "select T1.o_custkey, T1.num1, T2.num2 from (select o_custkey, sum(o_totalprice) as num1 from orders where o_orderpriority='3-MEDIUM' group by o_custkey) as T1,(select o_custkey, COUNT(o_custkey) as num2 from orders where o_orderdate='1996-01-02' group by o_custkey) as T2 where T1.o_custkey=T2.o_custkey";
    public static final String Q2 = "(select o_custkey, sum(o_totalprice) as num from orders where o_orderpriority='3-MEDIUM' group by o_custkey) UNION ALL (select o_custkey, COUNT(o_custkey) as num from orders where o_orderdate='1996-01-02' group by o_custkey)";
    public static final String Q3 = "(select o_custkey, o_comment from orders where o_orderpriority='3-MEDIUM') UNION ALL (select o_custkey, o_comment from orders where o_orderdate='1996-01-02')";
    public static final String Q4 = "select o_custkey, o_comment from orders where o_orderpriority='3-MEDIUM'";
    public static final String Q5 = "(select o_custkey, o_comment as teststring from orders where o_orderdate='1996-01-02') UNION ALL (select o_custkey, o_orderpriority as teststring from orders where o_orderdate='1996-01-02')";
}
