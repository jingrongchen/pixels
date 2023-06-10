package io.pixelsdb.pixels.parser;

public abstract class TestQuery {
    
    public static final String Q1 = "select T1.o_custkey, T1.num1, T2.num2 from (select o_custkey, sum(o_totalprice) as num1 from orders where o_orderpriority='3-MEDIUM' group by o_custkey) as T1,(select o_custkey, COUNT(o_custkey) as num2 from orders where o_orderdate='1996-01-02' group by o_custkey) as T2 where T1.o_custkey=T2.o_custkey";
    public static final String Q2 = "(select o_custkey, sum(o_totalprice) as num from orders where o_orderpriority='3-MEDIUM' group by o_custkey) UNION ALL (select o_custkey, COUNT(o_custkey) as num from orders where o_orderdate='1996-01-02' group by o_custkey)";
    public static final String Q3 = "(select o_custkey, o_comment from orders where o_orderpriority='3-MEDIUM') UNION ALL (select o_custkey, o_comment from orders where o_orderdate='1996-01-02')";
    public static final String Q4 = "select o_custkey, o_comment from orders where o_orderpriority='3-MEDIUM'";
    public static final String Q5 = "(select o_custkey, o_comment as teststring from orders where o_orderdate='1996-01-02') UNION ALL (select o_custkey, o_orderpriority as teststring from orders where o_orderdate='1996-01-02')";
    public static final String Q6 = "(select o_orderkey as key, l_extendedprice as num from orders, lineitem where o_orderkey=l_orderkey) UNION ALL (select l_orderkey as key, ps_supplycost as num from lineitem, partsupp where l_suppkey=ps_suppkey)";
    public static final String Q7 = "(select o_custkey, sum(o_totalprice) as num from orders where o_orderpriority='3-MEDIUM' group by o_custkey) UNION ALL (select o_custkey, COUNT(o_custkey) as num from orders where o_orderpriority='3-MEDIUM' group by o_custkey)";
    public static final String TPCHQ1 = "select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as sum_base_price, sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, avg(l_quantity) as avg_qty, avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order from  lineitem where l_shipdate <= date '1998-12-01' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus LIMIT 1";

    // public static final String Q6 = "(select ss_customer_sk, sr_item_sk from store_sales,store_returns where sr_item_sk=ss_item_sk) union all (select ss_customer_sk, ss_item_sk from store_sales,customer where ss_hdemo_sk=c_current_cdemo_sk)";
    // public static final String Q7 = " select * from (select * from store_sales,store_returns where sr_item_sk=ss_item_sk and ss_customer_sk=sr_customer_sk ) as join1, (select * from store_sales,customer where ss_hdemo_sk=c_current_cdemo_sk and ss_addr_sk=c_current_addr_sk ) as join2 where join1.ss_item_sk=join2.ss_item_sk and join1.ss_store_sk>30 and join2.c_current_cdemo_sk";
    // public static final String Q8 = "(select ss_customer_sk, sum(store_returns.sr_return_quantity ) from store_sales,store_returns where sr_item_sk=ss_item_sk group by ss_customer_sk) union all (select ss_customer_sk, sum(store_sales.ss_quantity) from store_sales,customer where ss_hdemo_sk=c_current_cdemo_sk group by ss_customer_sk)";

}
