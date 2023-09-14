package io.pixelsdb.pixels.planner.plan.logical;
import com.google.common.base.Objects;
import com.google.common.collect.ObjectArrays;

/**
 * @author Jingrong
 * @date 06/09/2023
 */
public class FusionTable implements Table {
    private final String schemaName;
    private final String tableName;
    private final String tableAlias;
    private final String[] join_columnNames;
    private final String[] agg_columnNames;
    private final Join join;
    private final Aggregation aggregation;
    
    /**
     * The {@link AggregatedTable#columnNames} of this class is constructed by the colum alias
     * of the origin table on which the aggregation is computed and the column alias of the
     * aggregation result columns that are computed by the aggregation functions.
     *
     * @param schemaName the schema name
     * @param tableName the table name
     * @param tableAlias the table alias
     * @param aggregation the information of the aggregation
     */
    public FusionTable(String schemaName, String tableName, String tableAlias, Join join, Aggregation aggregation)
    {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.tableAlias = tableAlias;

        //TODO: the fusion columns need to be read from the join result
        // this.columnNames =  ObjectArrays.concat(aggregation.getGroupKeyColumnAlias(), aggregation.getResultColumnAlias(), String.class);
        this.join = join;
        this.aggregation = aggregation;

        if (join.getJoinEndian() == JoinEndian.SMALL_LEFT)
        {
            this.join_columnNames = ObjectArrays.concat(
                    join.getLeftColumnAlias(), join.getRightColumnAlias(), String.class);
            // add the aggregation key and result columns    
            this.agg_columnNames = ObjectArrays.concat(
                    aggregation.getGroupKeyColumnAlias(), aggregation.getResultColumnAlias(), String.class);
        
            
        }
        else
        {
            this.join_columnNames = ObjectArrays.concat(
                    join.getRightColumnAlias(), join.getLeftColumnAlias(), String.class);
            // add the aggregation key and result columns
            this.agg_columnNames = ObjectArrays.concat(
                    aggregation.getGroupKeyColumnAlias(), aggregation.getResultColumnAlias(), String.class);
        }

    }


    @Override
    public TableType getTableType()
    {
        return TableType.FUSED;
    }

    @Override
    public String getSchemaName()
    {
        return schemaName;
    }

    @Override
    public String getTableName()
    {
        return tableName;
    }

    @Override
    public String getTableAlias()
    {
        return tableAlias;
    }

    @Override
    public String[] getColumnNames()
    {
        return join_columnNames;
    }

    public Aggregation getAggregation()
    {
        return aggregation;
    }

    public String[] getAggColumnNames()
    {
        return agg_columnNames;
    }

    public Join getJoin()
    {
        return join;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FusionTable that = (FusionTable) o;
        return Objects.equal(schemaName, that.schemaName) &&
                Objects.equal(tableName, that.tableName) &&
                Objects.equal(tableAlias, that.tableAlias) &&
                Objects.equal(join_columnNames, that.join_columnNames) &&
                Objects.equal(agg_columnNames, that.agg_columnNames) &&
                Objects.equal(join, that.join) &&
                Objects.equal(aggregation, that.aggregation);
                
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(schemaName, tableName, tableAlias, join_columnNames,agg_columnNames, join, aggregation);
    }


}
