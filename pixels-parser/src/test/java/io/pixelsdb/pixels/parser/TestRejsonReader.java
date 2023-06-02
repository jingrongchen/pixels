package io.pixelsdb.pixels.parser;

import org.junit.Test;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.grpc.InternalConfigSelector.Result;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.InputStream;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
// import com.google.gson.JsonObject;
import com.jayway.jsonpath.JsonPath;
import java.util.ArrayList;
import java.util.HashMap;

// import DAG

import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.jgrapht.graph.DefaultEdge;

import org.json.JSONObject;
        // 1.we need to group operation
        // 2.directly put them into stage or first group them?
        // 3.find the root for the filter and project and aggregate
        // 4.find the root for the join
        // 5.it's more reasonable to generate the stage and then the lambda json?
        // 6. 

        // plan 1.  every operator has inputs, then we try to change the inputs of these operators if they have the same input
        // 2.then we group them if they have the same inputs, and then we can generate the stage
        // 3. we need to find the root of the filter and project and aggregate
        // 4. we need to find the root of the join


        //plan2. every time we process a operator, we put them into stage, and then we process the next operator.
        
        // 1. grouping the operators
        // 2.write the result json

public class TestRejsonReader {
    
    @Test
    public void testRejsonReader() throws Exception {

        LambdaPlanner lambdaPlanner = new LambdaPlanner(Paths.get("/home/ubuntu/opt/pixels/pixels-parser/src/test/java/io/pixelsdb/pixels/parser/testlogicalPlan.json"));
        lambdaPlanner.generateDAG();
        // lambdaPlanner.plan();


    }

    public final class LambdaPlanner{
        private JsonNode jsonNode;
        // private List<JsonNode> lambdaRels = new ArrayList<>();
        private ObjectMapper mapper = new ObjectMapper();
        
        // For generateDAG()
        private DirectedAcyclicGraph<Integer,DefaultEdge> OptDag = new DirectedAcyclicGraph<Integer,DefaultEdge>(DefaultEdge.class);
        private HashMap<Integer,JsonNode> relIdToNode = new HashMap<Integer,JsonNode>();



        private List<JSONObject> tableInfoList = new ArrayList<JSONObject>();
        private HashMap<String,Integer> checkTbaleExist = new HashMap<String,Integer>();
        private List<Integer> processedRelId = new ArrayList<Integer>();


        public LambdaPlanner(JsonNode jsonNode) {
            this.jsonNode = jsonNode;
        }

        public LambdaPlanner(Path jsonPath) {
            this.jsonNode = readAsTree(jsonPath);
        }

        public void generateDAG() throws IOException{
            AtomicReference<Integer> last_rel_id = new AtomicReference<>(0);
            List<Integer> tableScanNode= new ArrayList<Integer>();
            // Build the DAG
            jsonNode.get("rels").forEach(rel -> {
                Integer relId = rel.get("id").asInt();
                String relOp= rel.get("relOp").asText();
                Integer last_reiId = Integer.parseInt( last_rel_id.toString());

                relIdToNode.put(relId,rel);
                
                if (relOp.equals("LogicalTableScan")) {
                    OptDag.addVertex(relId);
                    tableScanNode.add(relId);
                } else if (relOp.equals("LogicalJoin")||relOp.equals("LogicalUnion")) {
                    try {
                        List<String> joinInputs = mapper.readerForListOf(String.class).readValue(rel.path("inputs"));
                        OptDag.addVertex(relId);
                        joinInputs.forEach(joinInput -> {
                            Integer joinInputId = Integer.parseInt(joinInput);
                            OptDag.addEdge(joinInputId, relId);
                        });
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else {
                    OptDag.addVertex(relId);
                    OptDag.addEdge(last_reiId, relId);
                }

                last_rel_id.getAndUpdate(value -> relId);
            });

            //Check if we can reuse the table scan, find out same table scan
            HashMap<String,List<Integer>> tableScanMap = new HashMap<String,List<Integer>>();
            tableScanNode.forEach(tablescanID -> { 
                String tableName = relIdToNode.get(tablescanID).path("table").get(1).asText();
                if (tableScanMap.containsKey(tableName)) {
                    tableScanMap.get(tableName).add(tablescanID);
                } else {
                    List<Integer> tableScanList = new ArrayList<Integer>();
                    tableScanList.add(tablescanID);
                    tableScanMap.put(tableName, tableScanList);
                }
            });

            tableScanMap.forEach((tableName,tableScanList) -> {
                if (tableScanList.size() > 1) {
                    Integer firstTableScan = tableScanList.get(0);
                    tableScanList.remove(0);
                    tableScanList.forEach(tableScan -> {

                        OptDag.outgoingEdgesOf(tableScan).forEach(edge -> {
                            System.out.println("connect test");
                            OptDag.addEdge(firstTableScan, OptDag.getEdgeTarget(edge));
                        });
                        
                        OptDag.removeVertex(tableScan);
                    });
                }
            });
            
            // Check if we can reuse the 


            
            // BreadthFirstIterator<Integer, DefaultEdge> bfi = new BreadthFirstIterator<>(OptDag);
            // while (bfi.hasNext()) {
            //     System.out.println( bfi.next() );
            // }

            System.out.println("descendant of 0: ");
            OptDag.outgoingEdgesOf(0).forEach(edge -> {
                System.out.println(OptDag.getEdgeTarget(edge));
            });


        }



        public void plan() throws IOException{
            AtomicReference<Integer> last_rel_id = new AtomicReference<>(0);

            jsonNode.get("rels").forEach(rel -> {
                try {
                //basic info
                String relOp= rel.get("relOp").asText();
                Integer relId = rel.get("id").asInt();
                    
                // LogicalTableScan
                if (relOp.equals("LogicalTableScan")) {
                    
                    List<String> tableList = mapper.readerForListOf(String.class).readValue(rel.path("table"));
                    // String schemaName = tableList.get(0);
                    String tableName = tableList.get(1);

                    if (checkTbaleExist.containsKey(tableName) == false) {
                        JSONObject tableinfoNode = new JSONObject()
                            .put("id", relId)
                            .put("base", true)
                            .put("tableName", tableName)
                            .put("storageInfo", new JSONObject().put("schema", "s3"));
                        tableInfoList.add(tableinfoNode);
                        checkTbaleExist.put(tableName,tableInfoList.size()-1);
                    } else {
                        String optId= rel.get("id").asText();
                        Integer tableIndex = checkTbaleExist.get(tableName);
                        JSONObject tableinfoNode = tableInfoList.get(tableIndex);
                        tableinfoNode.put("id", tableinfoNode.get("id").toString() + "," + optId);
                        //TODO: we should change the inputs of the operator?
                    }

                    processedRelId.add(relId);

                }
                
                // LogicalFilter
                if (relOp.equals("LogicalFilter")){
                    // For scan senrio
                    if ( processedRelId.contains(last_rel_id)) {   }




                }

                // LogicalProject
                if (relOp.equals("LogicalProject")){

                }

                // LogicalAggregate
                if (relOp.equals("LogicalAggregate")){

                }

                if (relOp.equals("LogicalJoin")){

                }

                if (relOp.equals("LogicalUnion")){

                }

                last_rel_id.getAndUpdate(value -> relId);
            }catch (Exception e) {
                
                // System.out.println(last_rel_id.toString());
                // Integer current_rel_id = rel.get("id").asInt();
                // System.out.println(rel.toString());
                // last_rel_id.getAndUpdate(value -> current_rel_id);  
                

            }

            });
        }

        public JsonNode readAsTree(Path jsonPath) {
            try {
                JsonNode jsonNode =  mapper.readTree(Files.newInputStream(jsonPath));
                return jsonNode;
            } catch (Exception e) {
                e.printStackTrace();
            }    
            return jsonNode;
        }
        

    }

}
