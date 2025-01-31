package io.pixelsdb.pixels.parser;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import com.google.common.collect.ImmutableSet;

import software.amazon.awssdk.internal.http.LowCopyListMap.ForBuildable;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rel.externalize.RelJson;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;

public class CustomRelJsonWriter implements RelWriter {
    //~ Instance fields ----------------------------------------------------------
  
    protected final JsonBuilder jsonBuilder;
    protected final RelJson relJson;
    private final IdentityHashMap<RelNode, String> relIdMap = new IdentityHashMap<>();
    protected final List<@Nullable Object> relList;
    private final List<Pair<String, @Nullable Object>> values = new ArrayList<>();
    private @Nullable String previousId;
  
    //~ Constructors -------------------------------------------------------------
  
    public CustomRelJsonWriter() {
      this(new JsonBuilder());
    }
  
    public CustomRelJsonWriter(JsonBuilder jsonBuilder) {
      this.jsonBuilder = jsonBuilder;
      relList = this.jsonBuilder.list();
      relJson = new RelJson(this.jsonBuilder);
    }
  
    //~ Methods ------------------------------------------------------------------
  
    protected void explain_(RelNode rel, List<Pair<String, @Nullable Object>> values) {
      final Map<String, @Nullable Object> map = jsonBuilder.map();
  
      map.put("id", null); // ensure that id is the first attribute
      map.put("relOp", relJson.classToTypeName(rel.getClass()));
      for (Pair<String, @Nullable Object> value : values) {
        if (value.right instanceof RelNode) {
          continue;
        }
        put(map, value.left, value.right);
      }
      // omit 'inputs: ["3"]' if "3" is the preceding rel
      final List<@Nullable Object> list = explainInputs(rel.getInputs());
      if (list.size() != 1 || !Objects.equals(list.get(0), previousId)) {
        map.put("inputs", list);
      }
  
      final String id = Integer.toString(relIdMap.size());
      relIdMap.put(rel, id);
      map.put("id", id);
  
      relList.add(map);
      previousId = id;
    }
  
    private void put(Map<String, @Nullable Object> map, String name, @Nullable Object value) {
      // if (name.equals("condition")){
      //     if (value instanceof RexCall){
      //         for(RexNode rexnode : ((RexCall) value).getOperands()){                
                
      //           if (rexnode instanceof RexSubQuery){
      //               // System.out.println("RexSubQuery name :"+ name);
      //               // System.out.println("RexSubQuery value :"+ ((RexSubQuery)rexnode).rel);


      //           } else {
      //             // map.put(name, relJson.toJson(rexnode));
      //             // System.out.println("else test");
      //             // System.out.println("rexnode"+rexnode.toString());
      //             map.put("testaa", "test");
      //           }

      //     }
      //   }
      // }

      // if (value instanceof ImmutableSet){
      //   map.put(name, relJson.toJson(((ImmutableSet) value).iterator().next()));
      //   // map.put(name, "test");
      // }else {
      //   map.put(name, relJson.toJson(value));
      // }
      map.put(name, relJson.toJson(value));
    }
    

    private List<@Nullable Object> explainInputs(List<RelNode> inputs) {
      final List<@Nullable Object> list = jsonBuilder.list();
      for (RelNode input : inputs) {
        String id = relIdMap.get(input);
        if (id == null) {
          input.explain(this);
          id = previousId;
        }
        list.add(id);
      }
      return list;
    }
  
    @Override public final void explain(RelNode rel, List<Pair<String, @Nullable Object>> valueList) {
      explain_(rel, valueList);
    }
  
    @Override public SqlExplainLevel getDetailLevel() {
      return SqlExplainLevel.ALL_ATTRIBUTES;
    }
  
    @Override public RelWriter item(String term, @Nullable Object value) {
      values.add(Pair.of(term, value));
      return this;
    }
  
    @Override public RelWriter done(RelNode node) {
      final List<Pair<String, @Nullable Object>> valuesCopy =
          ImmutableList.copyOf(values);
      values.clear();
      explain_(node, valuesCopy);
      return this;
    }
  
    @Override public boolean nest() {
      return true;
    }
  
    /**
     * Returns a JSON string describing the relational expressions that were just
     * explained.
     */
    public String asString() {
      final Map<String, @Nullable Object> map = jsonBuilder.map();
      map.put("rels", relList);
      return jsonBuilder.toJsonString(map);
    }
  }
  
