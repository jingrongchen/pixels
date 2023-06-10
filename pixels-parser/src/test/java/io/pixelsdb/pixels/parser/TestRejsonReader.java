package io.pixelsdb.pixels.parser;

import org.junit.Test;
import org.locationtech.jts.planargraph.Subgraph;
import org.locationtech.jts.triangulate.quadedge.Vertex;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.grpc.InternalConfigSelector.Result;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.InputStream;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Principal;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.lang.model.util.ElementScanner6;

import com.fasterxml.jackson.databind.node.ObjectNode;
// import com.google.gson.JsonObject;
import com.jayway.jsonpath.JsonPath;
import java.util.ArrayList;
import java.util.HashMap;

// import DAG
import com.fasterxml.jackson.databind.util.TokenBuffer;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.Graph;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.apache.calcite.runtime.Pattern.Op;
import org.jgrapht.graph.DefaultEdge;
import java.util.Arrays;
import org.json.JSONObject;
import java.util.Set;
import java.util.HashSet;
import org.jgrapht.graph.AsSubgraph;
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
        LambdaPlanner lambdaPlanner = new LambdaPlanner(Paths.get("/home/ubuntu/opt/pixels/pixels-parser/src/test/java/io/pixelsdb/pixels/parser/logicalplan/testlogicalPlanQ7.json"));
        lambdaPlanner.setS3Path("s3://jingrong-lambda-test/unit_tests/intermediate_result/", "s3://jingrong-lambda-test/unit_tests/final_results/");
        lambdaPlanner.generateDAG();
        lambdaPlanner.optDAG();
        lambdaPlanner.genSubGraphs();
    }

    public final class LambdaPlanner{
        private JsonNode jsonNode;
        // private List<JsonNode> lambdaRels = new ArrayList<>();
        private ObjectMapper mapper = new ObjectMapper();
        
        // For generateDAG()
        private DirectedAcyclicGraph<Integer,DefaultEdge> OptDag = new DirectedAcyclicGraph<Integer,DefaultEdge>(DefaultEdge.class);
        private HashMap<Integer,ObjectNode> relIdToNode = new HashMap<Integer,ObjectNode>();

        // intermediate folder Path
        private String intermediateDirPath;
        private String finalDirPath;

        // For geneDAg() and optDAG()
        private List<Integer> tableScanNode = new ArrayList<Integer>();
        private List<Integer> joinunionNode = new ArrayList<Integer>();
        private List<Integer> filterNode = new ArrayList<Integer>();
        private List<Integer> projectNode = new ArrayList<Integer>();
        private List<Integer> aggregateNode = new ArrayList<Integer>();

        //For subgraphs
        private List<Graph<Integer,DefaultEdge>> subGraphs = new ArrayList<Graph<Integer,DefaultEdge>>();

        public LambdaPlanner(JsonNode jsonNode) {
            this.jsonNode = jsonNode;
        }

        public LambdaPlanner(Path jsonPath) {
            this.jsonNode = readAsTree(jsonPath);
        }

        public void generateDAG() throws IOException{
            try{
                AtomicReference<Integer> last_rel_id = new AtomicReference<>(0);
                // Build the DAG
                jsonNode.get("rels").forEach(rel -> {
                    Integer relId = rel.get("id").asInt();
                    String relOp= rel.get("relOp").asText();
                    Integer last_reiId = Integer.parseInt( last_rel_id.toString());

                    relIdToNode.put(relId,(ObjectNode) rel);
                    
                    if (relOp.equals("LogicalTableScan")) {
                        OptDag.addVertex(relId);
                        tableScanNode.add(relId);
                    } else if (relOp.equals("LogicalJoin")||relOp.equals("LogicalUnion")) {
                        try {
                            joinunionNode.add(relId);
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
                        if (relOp.equals("LogicalFilter")) {
                            filterNode.add(relId);
                        } else if (relOp.equals("LogicalProject")) {
                            projectNode.add(relId);
                        } else if (relOp.equals("LogicalAggregate")) {
                            aggregateNode.add(relId);
                        }

                        OptDag.addVertex(relId);
                        OptDag.addEdge(last_reiId, relId);
                    }

                    last_rel_id.getAndUpdate(value -> relId);
                });
                
                // Set the output path for last node
                Integer last_relId = Integer.parseInt(last_rel_id.toString());
                String outputPath = finalDirPath + "final_output.pxl";
                relIdToNode.get(last_relId).put("outputPath", outputPath);
                
        }catch (Exception e) {
            e.printStackTrace();
        }

        }

        public void optDAG() {
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
                            OptDag.addEdge(firstTableScan, OptDag.getEdgeTarget(edge));
                        });
                        
                        OptDag.removeVertex(tableScan);
                    });
                }
            });
            
            // Combine the filter and project and aggregate
            if(filterNode.size()>=2){
                nodeContentEqualCheckAndChange(OptDag,filterNode,"condition");
            }
            // if(projectNode.size()>=2){
            //     nodeContentEqualCheckAndChange(OptDag, projectNode, "fields");
            // }
            if(aggregateNode.size()>=2){
                nodeContentEqualCheckAndChange(OptDag, aggregateNode, "aggs");
            }
            


            // 1. set output path for aggregation
            aggregateNode.forEach(aggregateID -> {
                String outputPath = intermediateDirPath + "aggregate_" + aggregateID.toString() + "_output.pxl";
                // System.out.println(outputPath);
                relIdToNode.get(aggregateID).put("outputPath", outputPath);
            });




            // Change Join or union inputs
            // Set inputpath for Join or union
            joinunionNode.forEach(joinUnionID -> {
                List<String> newJoinInputs = new ArrayList<String>();
                List<String> joinInputPath = new ArrayList<String>();
                OptDag.incomingEdgesOf(joinUnionID).forEach(edge -> {
                    newJoinInputs.add(OptDag.getEdgeSource(edge).toString());
                    joinInputPath.add(relIdToNode.get(OptDag.getEdgeSource(edge)).path("outputPath").asText());
                });
                relIdToNode.get(joinUnionID).put("inputs", newJoinInputs.toString());
                relIdToNode.get(joinUnionID).put("inputPath", joinInputPath.toString());

                System.out.println(relIdToNode.get(joinUnionID).path("inputPath"));
            });

            // set input output path for pipeline breakers


        }

        public void genSubGraphs(){
            // Traverse the graph and find the breaker node and delete the edges
            BreadthFirstIterator<Integer, DefaultEdge> bfi = new BreadthFirstIterator<>(OptDag);
            HashSet<DefaultEdge> edgesToRemove = new HashSet<>();
            List<Integer> brekerNode = new ArrayList<Integer>();
            // find the breaker node and edges to remove
            while (bfi.hasNext()) {
                Integer currentVertex = bfi.next();
                if (relIdToNode.get(currentVertex).path("relOp").asText().equals("LogicalAggregate")) {
                    // System.out.println("find the Logical filter operator");
                    brekerNode.add(currentVertex);
                    Set<DefaultEdge> outgoingEdges = OptDag.outgoingEdgesOf(currentVertex);
                    edgesToRemove.addAll(outgoingEdges);
                } else if (relIdToNode.get(currentVertex).path("relOp").asText().equals("LogicalUnion") 
                || relIdToNode.get(currentVertex).path("relOp").asText().equals("LogicalJoin")){
                    // System.out.println("find the Logical Union operator");
                    
                    Boolean isBreaker = true;
                    // Get parent of the currentvertex

                    Set<DefaultEdge> incomEdges = OptDag.incomingEdgesOf(currentVertex);
                    Integer parentVertex = OptDag.getEdgeSource(incomEdges.iterator().next());
                    
                    if (relIdToNode.get(parentVertex).path("relOp").asText().equals("LogicalTableScan")){
                        isBreaker = false;
                    }
                    
                    if (isBreaker){
                        brekerNode.add(currentVertex);
                        Set<DefaultEdge> incomingEdges = OptDag.incomingEdgesOf(currentVertex);
                        edgesToRemove.addAll(incomingEdges);
                    }
                } else {
                    continue;
                }
                // System.out.println(currentVertex);
            }
            
            // delete edges
            System.out.println("breaker node"+ brekerNode);
            System.out.println("edges to delete" + edgesToRemove);               
            for (DefaultEdge edge : edgesToRemove) {
                OptDag.removeEdge(edge);
            }
            
            // Seperate the graph vertex into graphlist
            Set<Integer> vertexSet = new HashSet<Integer>(OptDag.vertexSet());
            System.out.println("node: "+vertexSet);
            List<HashSet<Integer>> graphList = new ArrayList<HashSet<Integer>>();
            brekerNode.forEach(
                node -> {

                    if (OptDag.inDegreeOf(node) == 0 && OptDag.outDegreeOf(node)!=0) {
                        Set<Integer> descSet = OptDag.getDescendants(node);
                        descSet.add(node);
                        graphList.add(new HashSet<Integer>(descSet));
                        vertexSet.removeAll(descSet);

                    } else if (OptDag.outDegreeOf(node) == 0 && OptDag.inDegreeOf(node)!=0) {
                        Set<Integer> ancSet = OptDag.getAncestors(node);
                        ancSet.add(node);

                    //  System.out.println("in the middle if condition ancSet: "+ancSet);

                        Integer rootNodeId = ancSet.iterator().next();
                    //  System.out.println("rootNodeId: "+rootNodeId);

                        if (graphList.size()!=0){
                            graphList.forEach(graph -> {
                                if (graph.contains(rootNodeId)){
                                    graph.addAll(ancSet);
                                } else {
                                    graphList.add(new HashSet<Integer>(ancSet));
                                }
                                vertexSet.removeAll(ancSet);
                            });
                        } else {
                            graphList.add(new HashSet<Integer>(ancSet));
                            vertexSet.removeAll(ancSet);
                            ancSet.forEach(anc -> {
                                vertexSet.remove(anc);
                            });

                        //  System.out.println("deleted");
                        }
                    } else {
                        Set<Integer> nodeset= new HashSet<Integer>();
                        nodeset.add(node);
                        graphList.add(new HashSet<Integer>(nodeset));
                        vertexSet.remove(node);
                    }
                }
            );


            if (vertexSet.size()!=0){
                System.out.println("add vertexSet to graphlist" + vertexSet);
                graphList.add(new HashSet<Integer>(vertexSet));
            }

            // Build subgraphs
            
            graphList.forEach(graph -> {
                subGraphs.add(new AsSubgraph<>(OptDag, graph));
            });

            subGraphs.forEach(subGraph -> {
                System.out.println("subGraph vertexSet: " + subGraph.vertexSet() + " edgeSet: " + subGraph.edgeSet());
            });
            
            // System.out.println("subGraphs: "+subGraphs.get(subGraphs.size()-1).vertexSet());
            // System.out.println(relIdToNode.get(8).path("outputPath").asText());

        }

        public void nodeContentEqualCheckAndChange(DirectedAcyclicGraph<Integer,DefaultEdge> OptDag ,List<Integer> nodesId, String condition) {
            List<ObjectNode> nodes = new ArrayList<ObjectNode>();
            nodesId.forEach(nodeId -> {
                nodes.add(relIdToNode.get(nodeId));
            });
            
            for(int i = 0; i < nodes.size(); i++) {
                for(int j = i+1; j < nodes.size(); j++) {
                    boolean check = nodes.get(i).path(condition).equals(nodes.get(j).path(condition));
                    if (check) {
                        int baseNodeId = i;
                        OptDag.outgoingEdgesOf(nodes.get(j).path("id").asInt()).forEach(edge -> {
                            OptDag.addEdge(nodes.get(baseNodeId).path("id").asInt(), OptDag.getEdgeTarget(edge));
                        });
                        OptDag.removeVertex(nodes.get(j).path("id").asInt());
                        System.out.println("equal and change the node");

                    } else {
                        continue;
                    }
                }
            }

        }

        public void setS3Path(String intermediateDirPath, String finalDirPath) {
            this.intermediateDirPath = intermediateDirPath;
            this.finalDirPath = finalDirPath;
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
        
        public void invokeLambda() {
            // invoke lambda based on subgraphs
            subGraphs.forEach(subGraph -> {
                // invoke lambda

                
            });

        }
    }

}
