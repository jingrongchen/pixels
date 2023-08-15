package io.pixelsdb.pixels.parser;

import org.junit.Test;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.HashMap;
import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

// import DAG
// import com.fasterxml.jackson.databind.util.TokenBuffer;
// import com.google.common.collect.BoundType;
// import com.google.protobuf.ByteString.Output;

import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.Graph;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.eclipse.jetty.server.Response.OutputType;
import org.jgrapht.graph.DefaultEdge;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.HashSet;
import org.jgrapht.graph.AsSubgraph;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.pixelsdb.pixels.planner.plan.physical.input.ThreadScanInput;
import io.pixelsdb.pixels.planner.plan.physical.output.ScanOutput;
import java.util.concurrent.FutureTask;

import io.grpc.netty.shaded.io.netty.channel.local.LocalAddress;
import io.pixelsdb.pixels.common.lock.EtcdAutoIncrement;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.common.physical.StorageFactory;
import io.pixelsdb.pixels.common.turbo.Input;
import io.pixelsdb.pixels.common.turbo.InvokerFactory;
import io.pixelsdb.pixels.common.turbo.WorkerType;
import io.pixelsdb.pixels.core.TypeDescription;

import com.fasterxml.jackson.databind.ObjectMapper; 
import com.fasterxml.jackson.databind.ObjectWriter; 
import com.fasterxml.jackson.core.type.TypeReference;

//invoke lambda
import io.pixelsdb.pixels.executor.predicate.TableScanFilter;
import io.pixelsdb.pixels.planner.plan.physical.domain.AggregationInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.InputSplit;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartialAggregationInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ScanTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.StorageInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.ThreadScanTableInfo;
import java.util.SortedMap;
import java.util.TreeMap;
import io.pixelsdb.pixels.executor.predicate.ColumnFilter;
import java.util.Map;  
import io.pixelsdb.pixels.executor.predicate.Filter;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.executor.predicate.Bound;
import io.pixelsdb.pixels.planner.plan.physical.domain.ThreadOutputInfo;
import java.util.concurrent.Future;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;

import io.pixelsdb.pixels.common.turbo.Output;
import java.util.Collections;

import io.pixelsdb.pixels.worker.common.BaseAggregationWorker;
import io.pixelsdb.pixels.worker.common.BaseJoinScanFusionWorker;
import io.pixelsdb.pixels.worker.common.BasePartitionWorker;
///for local invoking
import io.pixelsdb.pixels.worker.common.BaseThreadScanWorker;
import io.pixelsdb.pixels.worker.common.WorkerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Iterator;

//for aggregation
import io.pixelsdb.pixels.planner.plan.physical.input.AggregationInput;
import io.pixelsdb.pixels.planner.plan.physical.input.JoinScanFusionInput;
import io.pixelsdb.pixels.planner.plan.physical.input.ScanInput;
import io.pixelsdb.pixels.planner.plan.physical.domain.AggregatedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.OutputInfo;
import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.FusionOutput;
//For join
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionInput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.planner.plan.physical.input.PartitionedJoinInput;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedTableInfo;
import io.pixelsdb.pixels.planner.plan.physical.domain.PartitionedJoinInfo;
import io.pixelsdb.pixels.executor.join.JoinType;
import io.pixelsdb.pixels.planner.plan.physical.domain.MultiOutputInfo;
import io.pixelsdb.pixels.worker.common.BasePartitionedJoinWorker;
import com.alibaba.fastjson.JSONArray;

public class TestRejsonReader {
    
    @Test
    public void testRejsonReader() throws Exception {
        LambdaPlanner lambdaPlanner = new LambdaPlanner(Paths.get("/home/ubuntu/opt/pixels/pixels-parser/src/test/java/io/pixelsdb/pixels/parser/logicalplan/TPCHQ18.json"));
        String hostaddr="ec2-18-218-128-203.us-east-2.compute.amazonaws.com";
        String schemaPath = "s3://jingrong-test/";

        lambdaPlanner.setConfigs(20,40);

        lambdaPlanner.setS3Path("s3://jingrong-lambda-test/unit_tests/intermediate_result/", "s3://jingrong-lambda-test/unit_tests/final_results/",schemaPath);
        lambdaPlanner.sethostAddr(hostaddr);
        lambdaPlanner.generateDAG();
        lambdaPlanner.optDAG();
        lambdaPlanner.genSubGraphs();
        // lambdaPlanner.invokeLamvscode-file://vscode-app/Users/johnchen/Desktop/Visual%20Studio%20Code.app/Contents/Resources/app/out/vs/code/electron-sandbox/workbench/workbench.htmlbda();

    }

    public final class LambdaPlanner{
        //For metadata
        private String hostAddr = null;

        private JsonNode jsonNode;
        // private List<JsonNode> lambdaRels = new ArrayList<>();
        private ObjectMapper mapper = new ObjectMapper();
        
        // For generateDAG()
        private DirectedAcyclicGraph<Integer,DefaultEdge> OptDag = new DirectedAcyclicGraph<Integer,DefaultEdge>(DefaultEdge.class);
        private HashMap<Integer,ObjectNode> relIdToNode = new HashMap<Integer,ObjectNode>();

        // intermediate folder Path
        private String intermediateDirPath;
        private String finalDirPath;
        private String schemaPath;

        // For geneDAg() and optDAG()
        private List<Integer> tableScanNode = new ArrayList<Integer>();
        private List<Integer> joinunionNode = new ArrayList<Integer>();
        private List<Integer> filterNode = new ArrayList<Integer>();
        private List<Integer> projectNode = new ArrayList<Integer>();
        private List<Integer> aggregateNode = new ArrayList<Integer>();

        //For subgraphs
        private List<Graph<Integer,DefaultEdge>> subGraphs = new ArrayList<Graph<Integer,DefaultEdge>>();


        // TODO:For configs, expecially for numFilesInEachPartition
        private int numCloudFunction = 10;
        private int numFilesInEachPartition = 10;
        private int parallelism = 0;
        private int numPartition = 40;

        public LambdaPlanner(JsonNode jsonNode) {
            this.jsonNode = jsonNode;
        }

        public LambdaPlanner(Path jsonPath) {
            this.jsonNode = readAsTree(jsonPath);
        }
        
        public void sethostAddr(String hostAddr){
            this.hostAddr = hostAddr;
        }

        public void setConfigs(Integer numCloudFunction,Integer parallelism){
            this.numCloudFunction = numCloudFunction;
            this.parallelism = parallelism;
        }

        public void generateDAG() throws IOException{
            System.out.println("start generateDAG");
            try{
                AtomicReference<Integer> last_rel_id = new AtomicReference<>(0);
                // Build the DAG
                jsonNode.get("rels").forEach(rel -> {
                    Integer relId = rel.get("id").asInt();
                    String relOp= rel.get("relOp").asText();
                    Integer last_reiId = Integer.parseInt( last_rel_id.toString());

                    System.out.println("relId: "+relId+" relOp: "+relOp+" last_rel_id: "+last_reiId);


                    relIdToNode.put(relId,(ObjectNode) rel);
                    
                    if (relOp.equals("LogicalTableScan")) {
                        OptDag.addVertex(relId);
                        tableScanNode.add(relId);
                    } else if (relOp.equals("LogicalJoin")||relOp.equals("LogicalUnion")) {
                        try {
                            joinunionNode.add(relId);
                            //TODO:POTENTIAL PROBLEM OF MAPPER TO LIST
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
        System.out.println("finish generateDAG");
        }

        public void optDAG() {
            System.out.println("Start optDAG");
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
                String outputPath = intermediateDirPath + "aggregate_" + aggregateID.toString() + "_output";
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

                // System.out.println(relIdToNode.get(joinUnionID).path("inputPath"));
            });


            // System.out.println("optDAG vertexSet: " + OptDag.vertexSet() + " optDAGedgeSet: " + OptDag.edgeSet());
            System.out.println("finish optDAG");
            // set input output path for pipeline breakers
            System.out.println("Graph vertexSet: " + OptDag.vertexSet() + " edgeSet: " + OptDag.edgeSet());


        }


        public List<Integer> findbreakerNode(){
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

                    // Set<DefaultEdge> incomEdges = OptDag.incomingEdgesOf(currentVertex);
                    // Integer parentVertex = OptDag.getEdgeSource(incomEdges.iterator().next());
                    
                    // if (relIdToNode.get(parentVertex).path("relOp").asText().equals("LogicalTableScan")){
                    //     isBreaker = false;
                    // }
                    
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
            return brekerNode;


        }

        public void dfsStopAtNode(Graph<Integer, DefaultEdge> graph, Integer startNode, Integer targetNode) {
            Stack<Integer> stack = new Stack<>();
            stack.push(startNode);
    
            while (!stack.isEmpty()) {
                Integer current = stack.pop();
                System.out.println("Visiting node: " + current);
    
                if (current.equals(targetNode)) {
                    System.out.println("Reached target node: " + targetNode);
                    break; // 停止遍历
                }
    
                for (DefaultEdge edge : graph.edgesOf(current)) {
                    Integer neighbor = Graphs.getOppositeVertex(graph, edge, current);
                    if (!neighbor.equals(startNode)) { // 避免回到起始节点
                        stack.push(neighbor);
                    }
                }
            }
        }
        
        public Set<Integer> bfsTraversal(Graph<Integer, DefaultEdge> graph, int startNode, int stopNode) {
            Queue<Integer> queue = new LinkedList<>();
            Set<Integer> visited = new HashSet<>();
            Map<Integer, Set<Integer>> parentMap = new HashMap<>();
    
            queue.add(startNode);
    
            while (!queue.isEmpty()) {
                int current = queue.poll();
                System.out.println("Visiting node: " + current);
                visited.add(current);
                
                if (current != stopNode) {
                    for (DefaultEdge edge : graph.outgoingEdgesOf(current)) {
                        int neighbor = Graphs.getOppositeVertex(graph, edge, current);
                        if (!visited.contains(neighbor)) {
                            queue.add(neighbor);
                            visited.add(neighbor);
                            parentMap.computeIfAbsent(current, k -> new HashSet<>()).add(neighbor);
                        }
                    }
                }

                if(current != startNode){
                    for (DefaultEdge edge : graph.incomingEdgesOf(current)){
                        int neighbor = Graphs.getOppositeVertex(graph, edge, current);
                        
                        if (!visited.contains(neighbor)) {
                            System.out.print("test neighbor: " + neighbor);
                            // queue.add(neighbor);
                            visited.add(neighbor);
                            parentMap.computeIfAbsent(neighbor, k -> new HashSet<>()).add(current);
                        }

                    }
                }


                if (current == stopNode) {
                    System.out.println("Reached stop node: " + stopNode);
                    break; // 到达终止节点，停止搜索
                }
            }
    
            // 输出终止节点的父节点
            for (Map.Entry<Integer, Set<Integer>> entry : parentMap.entrySet()) {
                System.out.println("Node: " + entry.getKey() + " - Parents: " + entry.getValue());
            }
            return visited;

        }
    
    
    
    
    
    
    
    
    
    

        public void genSubGraphs(){

            List<Graph<String, DefaultEdge>> graphList = new ArrayList<>();
            List<Integer> breakerNode = findbreakerNode();
            
            System.out.println("breakerNode: " + breakerNode);

            for (int i=0;i<breakerNode.size();i++){
                Set<Integer> subGraphNodes = null; 
                if((i+1<breakerNode.size()) && (breakerNode.get(i) < breakerNode.get(i+1))){
                    subGraphNodes=bfsTraversal(OptDag,breakerNode.get(i), breakerNode.get(i+1));
                }
            }


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

        public void setS3Path(String intermediateDirPath, String finalDirPath,String schemaPath) {
            this.intermediateDirPath = intermediateDirPath;
            this.finalDirPath = finalDirPath;
            this.schemaPath = schemaPath;
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

        public boolean checkPartialAggregation(Graph<Integer,DefaultEdge> subgraph){
            boolean partialAggregation = false;
            for (Integer vertex : subgraph.vertexSet()) {
                ObjectNode node = relIdToNode.get(vertex);
                String operands = node.path("relOp").asText();
                if (operands.equals("LogicalAggregate")){
                    partialAggregation = true;
                }
            }
            return partialAggregation;
        }

        public void invokeLambda() {            

            LinkedBlockingQueue<PartialAggregationInfo> partialAggregationInfos = new LinkedBlockingQueue<PartialAggregationInfo>();
            LinkedBlockingQueue<ThreadScanInput> scanInputs = new LinkedBlockingQueue<ThreadScanInput>(1);

            ExecutorService threadPool = Executors.newFixedThreadPool(1);
            
            LambdaThread tester = new LambdaThread(subGraphs.get(0), schemaPath, hostAddr, numCloudFunction, partialAggregationInfos, scanInputs);
            FutureTask<List<CompletableFuture>> futureTasktest = new FutureTask<>(tester);
            
            
            boolean test = true;
            if(test){

            boolean partiAggExist = checkPartialAggregation(subGraphs.get(0));
            
            System.out.println("partiAggExist: " + partiAggExist);
            /*** 
             * Stage 1: Scan
             * 
             * ***/
            try
            {
                threadPool.submit(futureTasktest);
                
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }

            try
            {   
                List<CompletableFuture> futures = futureTasktest.get();
                for (CompletableFuture future : futures) {
                    try {
                        System.out.println("future.get(): "+future.get());
                    } catch (Exception e) {
                        continue;
                        // System.out.println("future.get null");
                    }
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
            
            
            /*** 
             * Stage 2: Partial aggregation to Full aggregation
             *
             * This stage join the full aggregation
             * The aggregation information are in the last stage subgraph             * 
             * We need to get the aggregation information from the last stage subgraph
             * 
             * ***/
            
            if (partiAggExist){
                Integer sizeOfAggregation = partialAggregationInfos.size();
                List<AggregationInput> aggregationInputs = new ArrayList<AggregationInput>();
                Storage storage = null;
                ThreadScanInput scaninput = scanInputs.poll();

                try{
                    storage = StorageFactory.Instance().getStorage(Storage.Scheme.s3);
                    for(int i=0;i<sizeOfAggregation;i++){
                        PartialAggregationInfo temPartialAggregationInfo = partialAggregationInfos.take();
                        //set aggregationInfo
                        AggregatedTableInfo aggregatedTableInfo = new AggregatedTableInfo();
                        aggregatedTableInfo.setParallelism(1);
                        aggregatedTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null,null, null, null));
                        aggregatedTableInfo.setInputFiles(storage.listPaths(scaninput.getOutput().getPath().get(i)));

                        String [] colToRead = new String[temPartialAggregationInfo.getGroupKeyColumnAlias().length + temPartialAggregationInfo.getResultColumnAlias().length];
                        System.arraycopy(temPartialAggregationInfo.getGroupKeyColumnAlias(), 0, colToRead, 0, temPartialAggregationInfo.getGroupKeyColumnAlias().length);
                        System.arraycopy(temPartialAggregationInfo.getResultColumnAlias(), 0, colToRead, temPartialAggregationInfo.getGroupKeyColumnAlias().length, temPartialAggregationInfo.getResultColumnAlias().length);

                        printArray("test colToread", colToRead);

                        aggregatedTableInfo.setColumnsToRead(colToRead);
                        aggregatedTableInfo.setBase(false);
                        aggregatedTableInfo.setTableName("aggregate_"+scaninput.getTableInfo().getTableName());

                        //set aggregationInput
                        AggregationInput aggregationInput = new AggregationInput();
                        aggregationInput.setTransId(UUID.randomUUID().getMostSignificantBits() & Integer.MAX_VALUE);
                        aggregationInput.setAggregatedTableInfo(aggregatedTableInfo);

                        //set aggregationInfo
                        AggregationInfo aggregationInfo = new AggregationInfo();
                        aggregationInfo.setGroupKeyColumnIds(temPartialAggregationInfo.getGroupKeyColumnIds());
                        aggregationInfo.setAggregateColumnIds(temPartialAggregationInfo.getAggregateColumnIds());
                        aggregationInfo.setGroupKeyColumnNames(temPartialAggregationInfo.getGroupKeyColumnAlias());
                        
                        boolean[] groupKeyColumnProjection = new boolean[temPartialAggregationInfo.getGroupKeyColumnAlias().length];
                        Arrays.fill(groupKeyColumnProjection, true);
                        aggregationInfo.setGroupKeyColumnProjection(groupKeyColumnProjection);
                        aggregationInfo.setResultColumnNames(temPartialAggregationInfo.getResultColumnAlias());
                        aggregationInfo.setResultColumnTypes(temPartialAggregationInfo.getResultColumnTypes());
                        aggregationInfo.setFunctionTypes(temPartialAggregationInfo.getFunctionTypes());
                        aggregationInput.setAggregationInfo(aggregationInfo);
                        String outputPath = finalDirPath + scaninput.getTransId() + "_aggregation_" + i + "_output";
                        aggregationInput.setOutput(new OutputInfo(outputPath,
                            new StorageInfo(Storage.Scheme.s3, null, null, null, null), true));

                        
                        // AggregationOutput output = (AggregationOutput) InvokerFactory.Instance()
                        // .getInvoker(WorkerType.AGGREGATION).invoke(aggregationInput).get();
                        
                        AggregationOutput opt = invokeLocalAgg(aggregationInput);

                        System.out.println(opt);
                        // System.out.println("aggregationInput: " + output);
                    }

                }catch (Exception e){
                    e.printStackTrace();
                }
                
            }

            }   
        }

        public AggregationOutput invokeLocalAgg(AggregationInput aggregationInput) {
                
            WorkerMetrics workerMetrics = new WorkerMetrics();
            Logger logger = LogManager.getLogger(TestRejsonReader.class);
            WorkerContext workerContext = new WorkerContext(logger, workerMetrics, "123456");
            BaseAggregationWorker baseWorker = new BaseAggregationWorker(workerContext);
            return baseWorker.process(aggregationInput);
            // return CompletableFuture.supplyAsync(() -> {
            //     try {
            //         return baseWorker.process(aggregationInput);
            //     } catch (Exception e) {
            //         e.printStackTrace();
            //     }
            //     return null;
            // });                
        }
        
        public < E > void printArray(String description,E[] inputArray )
        {   
            System.out.printf("%s: ", description);

            for (E element : inputArray)
            {
                System.out.printf("%s ", element);
            }

            System.out.println();

        }

        public <T> T deepCopy(T original) {
            try {
                // Serialize the original object to JSON
                String json = mapper.writeValueAsString(original);
                // Deserialize the JSON back into a new instance of the object
                return mapper.readValue(json, (Class<T>) original.getClass());
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }

        class LambdaThread implements Callable{
            private MetadataService instance = null;
            private Graph<Integer,DefaultEdge> subGraph;
            private String schemaPath;
            private Integer parallism;
            private LinkedBlockingQueue<PartialAggregationInfo> partialAggregationInfos;
            private LinkedBlockingQueue<ThreadScanInput> scanInputs;
            public LambdaThread(Graph<Integer,DefaultEdge> subGraph, String schemaPath, String hostAddr, Integer parallism,LinkedBlockingQueue<PartialAggregationInfo> partialAggregationInfos,LinkedBlockingQueue<ThreadScanInput> scanInputs) {
                this.subGraph = subGraph;
                this.schemaPath = schemaPath;
                this.parallism = parallism;
                this.partialAggregationInfos = partialAggregationInfos;
                this.scanInputs = scanInputs;
                init(hostAddr);
            }

            public void init(String hostAddr)
            {
                this.instance = new MetadataService(hostAddr, 18888);
            }

            public void shutdown() throws InterruptedException
            {
                this.instance.shutdown();
            }


            @Override
            public List<CompletableFuture> call(){   
                ThreadScanInput scaninput = new ThreadScanInput();
                // AtomicReferenceArray<CompletableFuture> ScanOutputs = null;
                ConcurrentMap<Integer, CompletableFuture> scanOutputs = null;
                List<CompletableFuture> futures = new ArrayList<CompletableFuture>();
                

                if(checkHasOperation(subGraph, "LogicalJoin")){
                    executeJoinSubgraph();
                    // futures.add(executeJoinSubgraph());
                }

                if(checkHasOperation(subGraph, "LogicalTableScan") && !checkHasOperation(subGraph, "LogicalJoin")){
                    scaninput.setTransId(UUID.randomUUID().getMostSignificantBits() & Integer.MAX_VALUE);
                    ThreadScanTableInfo tableInfo = new ThreadScanTableInfo();
                    tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null, null,null, null));
                    Integer filterId = 1;
                    Integer projectId = 0;
                    Set<String> columnstoread = new HashSet<String>();
                    List<ColumnFilter> filterlist = new ArrayList<ColumnFilter>();
                    List<PartialAggregationInfo> aggregationlist = new ArrayList<PartialAggregationInfo>();
                    List<String> outputList = new ArrayList<String>();
                    HashMap<String, List<Integer>> filterOnAggreation = new HashMap<String, List<Integer>>();
                    
                    //for scanProjection
                    HashMap<String, List<Boolean>> scanProjection = new HashMap<String, List<Boolean>>();
                    HashMap<Integer, List<String>> scanProjectField = new HashMap<Integer, List<String>>();
                    
                    // GET TABLE COLUMN SCHEMA
                    // PixelsSchema tpchSchema = new PixelsSchema(tableInfo.getSchemaName(), this.instance);
                    // Map<String, org.apache.calcite.schema.Table> tableMap = tpchSchema.getTableMap();
                    // PixelsTable customerTable = (PixelsTable) tableMap.get(tableInfo.getTableName());
                    // RelDataType rowType = customerTable.getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
                    // List<RelDataTypeField> fieldList = rowType.getFieldList();
                    // GET TABLE COLUMN SCHEMA

                    List<String> paths = new ArrayList<String>();
                    // List<Integer> getLastElementList = new ArrayList<>(subGraph.vertexSet());
                    // Integer lastElement = getLastElementList.get(getLastElementList.size() - 1);
                    for (Integer vertex : subGraph.vertexSet()) {
                        ObjectNode node = relIdToNode.get(vertex);
                        String operands = node.path("relOp").asText();
                        if (operands.equals("LogicalTableScan")) {
                            tableInfo.setSchemaName(node.path("table").get(0).asText());
                            tableInfo.setTableName(node.path("table").get(1).asText());
                             // TODO: HOW TO GET THE INPUTS FILE PATH ?????
                            try {
                                String storagepath = schemaPath + tableInfo.getSchemaName()+"/" + tableInfo.getTableName() + "/" + "v-0-order" + "/";
                                // System.out.println("storagepath: " + storagepath);
                                Storage storage = StorageFactory.Instance().getStorage(storagepath);
                                paths = storage.listPaths(storagepath);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }                       
                        } else if (operands.equals("LogicalFilter")) {
                            // add filter string into filter string list
                            // System.out.println("got an filter" + filterId);
                            addFilterStringToList(tableInfo, node.path("condition"), filterId, filterlist, columnstoread);
                            filterId++;
                        } else if (operands.equals("LogicalProject")) {
                            List<String> temp = new ArrayList<String>();
                            node.path("fields").forEach(field -> {
                                columnstoread.add(field.asText());
                                temp.add(field.asText());
                            });

                            // for (JsonNode field : node.path("fields")){
                            //     columnstoread.add(field.asText());
                            //     temp.add(field.asText());
                            // }
                            scanProjectField.put(projectId, temp);
                            // System.out.println("scanProjectField: " + scanProjectField);
                            projectId++;
                        } else if (operands.equals("LogicalAggregate")){
                            outputList.add(node.path("outputPath").asText());

                            scaninput.setPartialAggregationPresent(true);
                            PartialAggregationInfo aggregationInfo = new PartialAggregationInfo();
                            String funtiontype = node.path("aggs").get(0).path("agg").path("name").asText();
                            aggregationInfo.setFunctionTypes(new FunctionType[]{FunctionType.fromName(funtiontype)});  
                            
                            // setting the groupkeycolumnids
                            List<Integer> groupCollist = Arrays.asList(mapper.convertValue(node.path("group"), Integer[].class));
                            List<Integer> aggColList = Arrays.asList(mapper.convertValue(node.path("aggs").get(0).path("operands"), Integer[].class));
                  
                            List<String> groupColStringList = new ArrayList<String>();
                            List<String> aggColStringList = new ArrayList<String>();


                            Integer oprID = node.path("aggs").get(0).path("operands").path(0).asInt();
                            for(DefaultEdge e:subGraph.incomingEdgesOf(vertex)){
                                Integer source = subGraph.getEdgeSource(e);
                                ObjectNode sourcenode = relIdToNode.get(source);
                                if(sourcenode.path("relOp").asText().equals("LogicalProject")){
                                    List<String> temp = new ArrayList<String>();
                                    sourcenode.path("fields").forEach(field -> {
                                        temp.add(field.asText());
                                    });
                                    for(Integer i:groupCollist){
                                        groupColStringList.add(temp.get(i));
                                    }

                                    for(Integer j:aggColList){
                                        aggColStringList.add(temp.get(j));
                                    }
                                }
                            }

                            // System.out.println("groupColStringList: " + groupColStringList);
                            // System.out.println("aggColStringList: " + aggColStringList);

                            if (aggColStringList.size() == 0){
                                aggregationInfo.setAggregateColumnNames(groupColStringList);
                            } else {
                                aggregationInfo.setAggregateColumnNames(aggColStringList);
                            }
                            aggregationInfo.setGroupKeyColumnNames(groupColStringList);

                            // setting the groupkeycolumnids
                            // aggregationInfo.setAggregateColumnIds(aggregateColumnIds);
                            // aggregationInfo.setGroupKeyColumnIds(columnsids);
                            // TODO: the real coulumnAlias

                            aggregationInfo.setGroupKeyColumnAlias(groupColStringList.toArray(new String[groupColStringList.size()]));

                            aggregationInfo.setNumPartition(0);
                            aggregationInfo.setPartition(false);
                            aggregationInfo.setResultColumnAlias(new String[]{node.path("aggs").get(0).path("name").asText()});
                            aggregationInfo.setResultColumnTypes(new String[]{node.path("aggs").get(0).path("type").path("type").asText()});
                            
                            if (node.path("aggs").get(0).path("type").path("type").asText().equals("BIGINT")){
                                aggregationInfo.setResultColumnTypes(new String[]{TypeDescription.Category.valueOf("LONG").toString()});
                            }
                            // aggregationInfo.setResultColumnTypes(new String[]{TypeDescription.Category.valueOf(node.path("aggs").get(0).path("type").path("type").asText()).toString()});
                            aggregationlist.add(aggregationInfo);
                        }
                    } 

                    //set scanprojection
                    for(int i=0;i<projectId;i++){
                        List<Boolean> temp = new ArrayList<Boolean>(Arrays.asList(new Boolean[columnstoread.size()])); 
                        Collections.fill(temp, Boolean.FALSE);
                        List<String> tempString = scanProjectField.get(i);

                        System.out.println("tempString: " + tempString);

                        for(int j=0;j<tempString.size();j++){
                            Integer idInIncludcolmns = new ArrayList<String>(columnstoread).indexOf(tempString.get(j));
                            temp.set(idInIncludcolmns, Boolean.TRUE);
                        }

                        scanProjection.put(Integer.toString(i), temp);
                    }

                    scaninput.setScanProjection(scanProjection);
                    
                    //set aggregation id
                    List<String> columnstoreadArray = new ArrayList<String>(columnstoread);
                    setAggregateColumnIds(aggregationlist,columnstoreadArray,scanProjection);
                    partialAggregationInfos.addAll(aggregationlist);
                    
                    // String [] columnstoreadarray =;
                    tableInfo.setColumnsToRead(columnstoread.toArray(new String[columnstoread.size()]));
                    List<String> scanfilterlist = new ArrayList<String>();

                    for (int i=0; i<filterlist.size();i++){
                        SortedMap<Integer, ColumnFilter> columnFilters = new TreeMap<Integer, ColumnFilter>();
                        List<String> columnstoreadList = Arrays.asList(columnstoread.toArray(new String[columnstoread.size()]));

                        Integer idInIncludcolmns = columnstoreadList.indexOf(filterlist.get(i).getColumnName());

                        columnFilters.put(idInIncludcolmns, filterlist.get(i));
                        TableScanFilter scanfilter = new TableScanFilter(tableInfo.getSchemaName(), tableInfo.getTableName(), columnFilters);
                        String filterString = JSON.toJSONString(scanfilter);
                        scanfilterlist.add(filterString);
                    }

                    tableInfo.setFilter(scanfilterlist);
                    scaninput.setPartialAggregationInfo(aggregationlist);
                    
                    // TODO: filterOnAggreation should be fixed.
                    if(filterlist.size()>=2){
                        filterOnAggreation.put("0", Arrays.asList(0));
                        filterOnAggreation.put("1", Arrays.asList(1));
                        scaninput.setFilterOnAggreation(filterOnAggreation);
                    } else {
                        filterOnAggreation.put("0", Arrays.asList(0));
                        scaninput.setFilterOnAggreation(filterOnAggreation);
                    }

                    ExecutorService parallelCloudFunctionPool = Executors.newFixedThreadPool(parallism); 
                    int fileNumOnEachThread = (paths.size() + parallism -1) / parallism;
                    scanOutputs = new ConcurrentHashMap<Integer,CompletableFuture>(fileNumOnEachThread);

                    List<String> OutputList = new ArrayList<String>();
                        for (int k=0; k<outputList.size();k++){
                            OutputList.add(outputList.get(k)+"/");
                        }

                    ThreadOutputInfo Threadoutput = new ThreadOutputInfo(OutputList, false,
                        new StorageInfo(Storage.Scheme.s3, null,null, null, null), true);                    
                    scaninput.setOutput(Threadoutput);

                    for (int i=0; i<1; i++){
                        List<String> tempFileList = new ArrayList<String>();
                        for (int j = i * fileNumOnEachThread; j<(i+1)*fileNumOnEachThread; j++){
                            if(j<paths.size()){
                                tempFileList.add(paths.get(j));
                            }
                        }

                        List<InputSplit> myList = new ArrayList<InputSplit>();
                        for (String line : tempFileList) {
                            InputSplit temp= new InputSplit(Arrays.asList(new InputInfo(line, 0, -1)));
                            myList.add(temp);
                        }

                        ThreadScanTableInfo TempTableInfo = deepCopy(tableInfo);
                        TempTableInfo.setInputSplits(myList);
                        ThreadScanInput tempScanInput = deepCopy(scaninput);
                        tempScanInput.setTableInfo(TempTableInfo);

                        if(i==0){
                            scanInputs.add(tempScanInput);
                        }

                        // System.out.println(JSON.toJSONString(tempScanInput));
                        // CompletableFuture<Output> OutputFuture = InvokerFactory.Instance().getInvoker(WorkerType.THREAD_SCAN).invoke(tempScanInput);
                        // CompletableFuture<ScanOutput> OutputFuture = invokeLocalLambda(tempScanInput);
                        CompletableFuture<ScanOutput> OutputFuture = null;
                        futures.add(OutputFuture);                   
                    }

                    //
                    
                    parallelCloudFunctionPool.shutdown();

                    return futures;
                    // System.out.println("file size :" + paths.size())
                }

                return null;
                // return futures;
            }
     
            public void addFilterStringToList(ThreadScanTableInfo tableInfo, JsonNode condition, Integer filterId, List<ColumnFilter> filterlist, Set<String> columnstoread) {
                
                // SortedMap<Integer, ColumnFilter> columnFilters = new TreeMap<Integer, ColumnFilter>();
                // GET TABLE COLUMN SCHEMA
                PixelsSchema tpchSchema = new PixelsSchema(tableInfo.getSchemaName(), this.instance);
                Map<String, org.apache.calcite.schema.Table> tableMap = tpchSchema.getTableMap();
                PixelsTable customerTable = (PixelsTable) tableMap.get(tableInfo.getTableName());
                RelDataType rowType = customerTable.getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
                List<RelDataTypeField> fieldList = rowType.getFieldList();
                // GET TABLE COLUMN SCHEMA

                Integer columnId = condition.path("operands").get(0).path("input").asInt();
                String columnName = fieldList.get(columnId).getName();
                columnstoread.add(columnName);
                
                SqlTypeName columnType = fieldList.get(columnId).getType().getSqlTypeName();
        
                Class<?> javaType = TypeDescription.Category.valueOf(columnType.toString()).getExternalJavaType();
                TypeDescription.Category category = TypeDescription.Category.valueOf(columnType.toString());

                // depends on type of filter
                String filterType = condition.path("op").path("kind").asText();
                
                String opKind = condition.path("operands").get(1).path("op").path("name").asText();
                if (filterType.equals("EQUALS")){
                    if(opKind.equals("CAST")){
                        String literal = condition.path("operands").get(1).path("operands").get(0).path("literal").asText();
                        // System.out.println("literal: "+literal);
                        ArrayList emptyRange = new ArrayList<>();
                        Bound bound = new Bound(Bound.Type.INCLUDED, literal);
                        ArrayList<Bound> discreteValues = new ArrayList<>();
                        discreteValues.add(bound);
                        Filter filter = new Filter(javaType, emptyRange, discreteValues, false, false, false, false);
                        ColumnFilter columnFilter = new ColumnFilter(columnName, category, filter);
                        filterlist.add(columnFilter);
                    }
                }
                // TableScanFilter scanfilter = new TableScanFilter(tableInfo.getSchemaName(), tableInfo.getTableName(), columnFilters);
                // drop json filed filter and isempty
                // String filterString = JSON.toJSONString(scanfilter);
                // filterlist.add(filterString);
            }
            
            public CompletableFuture<Output> executeJoinSubgraph(){
                // Initiate some useful data structure
                PartitionedJoinInput joinInput = new PartitionedJoinInput();
                PartitionedJoinInfo joinInfo = new PartitionedJoinInfo();
                List<PartitionedTableInfo> partitionedTableInfos = new ArrayList<PartitionedTableInfo>();
                Integer sepSize = 0;
                Integer scanCount = 0;
                List<String> totalSchema = new ArrayList<String>();

                List<PartitionInput> partitionInputs = new ArrayList<PartitionInput>();
                HashMap<String,List<InputSplit>> tableToInputSplits = new HashMap<String,List<InputSplit>>();
                // List<ColumnFilter> filterlist = new ArrayList<ColumnFilter>();
                
                HashMap<Integer, ColumnFilter> filterList = new HashMap<Integer, ColumnFilter>();
                // String [] columnsToRead = new String[]{};

                HashMap<Integer,List<RelDataTypeField>> tableToSchema = new HashMap<Integer,List<RelDataTypeField>>(); 
                tableToSchema.put(0, new ArrayList<RelDataTypeField>());
                tableToSchema.put(1, new ArrayList<RelDataTypeField>());
                HashMap<Integer,Set<String>> columsToReadMap = new HashMap<Integer,Set<String>>();
                columsToReadMap.put(0, new HashSet<String>());
                columsToReadMap.put(1, new HashSet<String>());
                HashMap<Integer,List<String>> joinProjection = new HashMap<Integer,List<String>>();
                joinProjection.put(0, new ArrayList<String>());
                joinProjection.put(1, new ArrayList<String>());
                
                HashMap<Integer,String> joinkeyName = new HashMap<Integer,String>();
                HashMap<String,Integer> tableKeyId = new HashMap<String,Integer>();
                HashMap<Integer,String> IdToTable = new HashMap<Integer,String>();
               

                for (Integer vertex : subGraph.vertexSet()){
                    ObjectNode node = relIdToNode.get(vertex);
                    String oprName = node.path("relOp").asText();

                    if (oprName.equals("LogicalTableScan")){
                        String schemaName = node.path("table").get(0).asText();
                        String tableName = node.path("table").get(1).asText();

                        PartitionInput parinput = new PartitionInput();
                        parinput.setTransId(UUID.randomUUID().getMostSignificantBits() & Integer.MAX_VALUE);
                        joinInput.setTransId(parinput.getTransId());
                        ScanTableInfo tableInfo = new ScanTableInfo();
                        tableInfo.setTableName(tableName);
                        tableInfo.setSchemaName(schemaName);

                        PixelsSchema tpchSchema = new PixelsSchema(schemaName, this.instance);
                        Map<String, org.apache.calcite.schema.Table> tableMap = tpchSchema.getTableMap();
                        PixelsTable customerTable = (PixelsTable) tableMap.get(tableName);                
                        RelDataType rowType = customerTable.getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));
                        tableToSchema.get(scanCount).addAll(rowType.getFieldList());
                        
                        tableKeyId.put(tableName, scanCount);
                        IdToTable.put(scanCount, tableName);
                        scanCount++;

                        totalSchema.addAll(rowType.getFieldNames());
                        if (sepSize == 0){
                            sepSize = totalSchema.size();
                        }

                        try {
                            List<InputSplit> paths = new ArrayList<InputSplit>();
                            List<String> filePath = new ArrayList<String>();

                            String storagepath = schemaPath + schemaName+"/" + tableInfo.getTableName() + "/" + "v-0-order" + "/";
                            System.out.println("storagepath: " + storagepath);
                            Storage storage = StorageFactory.Instance().getStorage(storagepath);
                            filePath = storage.listPaths(storagepath);

                            for (String line : filePath) {
                                InputSplit temp= new InputSplit(Arrays.asList(new InputInfo(line, 0, -1)));
                                paths.add(temp);
                            }

                            tableToInputSplits.put(tableName, paths);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        tableInfo.setBase(true);
                        tableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null,null, null, null));
                        PartitionInfo partitionInfo = new PartitionInfo();
                        partitionInfo.setNumPartition(numPartition);

                        parinput.setTableInfo(tableInfo);
                        parinput.setPartitionInfo(partitionInfo);
                        // parinput.setOutput(new OutputInfo(intermediateDirPath + "partition_" + tableName , false,
                        //         new StorageInfo(Storage.Scheme.s3, null, null, null), true));
                        
                        partitionInputs.add(parinput);
                        // tableInfo.setColumnsToRead(new String[]{"l_orderkey", "l_suppkey", "l_extendedprice", "l_discount"});
                        // tableInfo.setFilter(filter);
                        // input.setProjection(new boolean[]{true, true, true, true});
                        // partitionInfo.setKeyColumnIds(new int[]{0});
                    } else if (oprName.equals("LogicalJoin")){
                        if(node.path("joinType").asText().equals("inner") ){
                            joinInfo.setJoinType(JoinType.EQUI_INNER);
                        }
                        try{
                            String joininputsString = node.path("inputs").textValue();
                            joininputsString = joininputsString.replace("[", "").replace("]", "").replace("\"", "").replace(" ", "");
                            List<Integer> joinInputs = Arrays.asList(mapper.convertValue(joininputsString.split(","), Integer[].class));

                            joinInputs.forEach((v) -> {
                                String tableName = relIdToNode.get(v).path("table").get(1).asText();
                                PartitionedTableInfo partitionedTableInfo = new PartitionedTableInfo();
                                partitionedTableInfo.setTableName(tableName);
                                partitionedTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null,null, null, null));
                                partitionedTableInfos.add(partitionedTableInfo);
                            });
                        }catch (Exception e) {
                            e.printStackTrace();
                        }

                    } else if (oprName.equals("LogicalFilter")){
                        JsonNode operands = node.path("condition").path("operands");

                        for (int i = 0; i< operands.size(); i++){
                            JsonNode current = operands.get(i);
                            if(current.path("operands").get(1).size() > 2){
                            // it's for the filter string. 
                                Integer columnId = current.path("operands").get(0).path("input").asInt();
                                String columnName = totalSchema.get(columnId);
                                List<RelDataTypeField> tempfieldList=null;

                                if (columnId < sepSize-1){
                                    columsToReadMap.get(0).add(columnName);
                                    tempfieldList=tableToSchema.get(0);   
                                } else {
                                    columsToReadMap.get(1).add(columnName);
                                    tempfieldList =tableToSchema.get(1);
                                }

                                JsonNode condition = current;
                                SqlTypeName columnType = tempfieldList.get(columnId).getType().getSqlTypeName();
                                Class<?> javaType = TypeDescription.Category.valueOf(columnType.toString()).getExternalJavaType();
                                TypeDescription.Category category = TypeDescription.Category.valueOf(columnType.toString());
                                String filterType = condition.path("op").path("kind").asText();
                                
                                String opKind = condition.path("operands").get(1).path("op").path("name").asText();
                                if (filterType.equals("EQUALS")){
                                    if(opKind.equals("CAST")){
                                        String literal = condition.path("operands").get(1).path("operands").get(0).path("literal").asText();
                                        ArrayList emptyRange = new ArrayList<>();
                                        Bound bound = new Bound(Bound.Type.INCLUDED, literal);
                                        ArrayList<Bound> discreteValues = new ArrayList<>();
                                        discreteValues.add(bound);
                                        Filter filter = new Filter(javaType, emptyRange, discreteValues, false, false, false, false);
                                        ColumnFilter columnFilter = new ColumnFilter(columnName, category, filter);
                                        if (columnId < sepSize-1){
                                            filterList.put(0, columnFilter);
                                        } else {
                                            filterList.put(1, columnFilter);
                                        }
                                    }
                                }
                            } else {
                            // for the join string.
                                for(int j = 0; j<current.path("operands").get(1).size(); j++){
                                    Integer columnId = current.path("operands").get(j).path("input").asInt();
                                    joinkeyName.put(j, totalSchema.get(columnId));
                                    columsToReadMap.get(j).add(totalSchema.get(columnId));
                                }    
                            }
                        }
                    } else if (oprName.equals("LogicalProject")){
                        for(int i=0;i<node.path("exprs").size();i++){
                            Integer columnId = node.path("exprs").get(i).path("input").asInt();
                            String columnName = totalSchema.get(columnId);

                            if (columnId <= sepSize-1){
                                columsToReadMap.get(0).add(columnName);
                                joinProjection.get(0).add(columnName);
                            } else {
                                columsToReadMap.get(1).add(columnName);
                                joinProjection.get(1).add(columnName);
                            }
                        }
                    }
                }

                // set columns to read
                for(int i=0;i<columsToReadMap.size();i++){
                    partitionInputs.get(i).getTableInfo().setColumnsToRead(columsToReadMap.get(i).toArray(new String[columsToReadMap.get(i).size()]));
                    boolean [] boolArray = new boolean[columsToReadMap.get(i).size()];
                    Arrays.fill(boolArray, true);
                    partitionInputs.get(i).setProjection(boolArray);
                }

                // set filter
                for(int i=0;i<filterList.size();i++){
                    SortedMap<Integer, ColumnFilter> columnFilters = new TreeMap<Integer, ColumnFilter>();
                    Integer idOfPartitionInput = Integer.parseInt(filterList.keySet().toArray()[i].toString());
                    ScanTableInfo tempTableInfo = partitionInputs.get(idOfPartitionInput).getTableInfo();
                    List<String> columnstoreadList = new ArrayList<String>();
                    columnstoreadList.addAll(columsToReadMap.get(idOfPartitionInput));
                    Integer idInIncludcolmns = columnstoreadList.indexOf(filterList.get(idOfPartitionInput).getColumnName());
                    columnFilters.put(idInIncludcolmns, filterList.get(idOfPartitionInput));
                    TableScanFilter scanfilter = new TableScanFilter(tempTableInfo.getSchemaName(), tempTableInfo.getTableName(), columnFilters);
                    String filterString = JSON.toJSONString(scanfilter);
                    partitionInputs.get(idOfPartitionInput).getTableInfo().setFilter(filterString);
                }

                for (int i=0;i<partitionInputs.size();i++){
                    String keycolmnName = joinkeyName.get(i);
                    List<String> processList = new ArrayList<String>();
                    processList.addAll(columsToReadMap.get(i));
                    Integer keycolumnId = processList.indexOf(keycolmnName);
                    partitionInputs.get(i).getPartitionInfo().setKeyColumnIds(new int[]{keycolumnId});

                    // if no filter, still need to set an empty filter
                    if (partitionInputs.get(i).getTableInfo().getFilter()==null){
                        SortedMap<Integer, ColumnFilter> tempcolumnFilters = new TreeMap<Integer, ColumnFilter>();
                        TableScanFilter tempscanfilter = new TableScanFilter(partitionInputs.get(i).getTableInfo().getSchemaName(), partitionInputs.get(i).getTableInfo().getTableName(), tempcolumnFilters);
                        String filterString = JSON.toJSONString(tempscanfilter);
                        partitionInputs.get(i).getTableInfo().setFilter(filterString);
                    }
                }

                //1. compare the size of InputSplits to decide which one is left table
                String [] keyset = tableToInputSplits.keySet().toArray(new String[tableToInputSplits.size()]);
                String leftTableKey = keyset[0];
                String rightTableKey = null;
                for(String key : keyset){
                    if(tableToInputSplits.get(key).size() < tableToInputSplits.get(leftTableKey).size()){
                        rightTableKey = leftTableKey;
                        leftTableKey = key;
                    }
                }

                //set left table
                PartitionedTableInfo leftTableInfo = new PartitionedTableInfo();
                leftTableInfo.setTableName(leftTableKey);
                leftTableInfo.setColumnsToRead(columsToReadMap.get(tableKeyId.get(leftTableKey)).toArray(new String[columsToReadMap.get(tableKeyId.get(leftTableKey)).size()]));
                leftTableInfo.setKeyColumnIds(partitionInputs.get(tableKeyId.get(leftTableKey)).getPartitionInfo().getKeyColumnIds());
                leftTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3, null,null, null, null));
                leftTableInfo.setBase(false);
                
                //TODO: smart parallelism setting   
                Integer leftParallelism = tableToInputSplits.get(leftTableKey).size()/numCloudFunction;
                leftTableInfo.setParallelism(leftParallelism);
                
                //left small right bigger
                PartitionedTableInfo rightTableInfo = new PartitionedTableInfo();
                rightTableInfo.setTableName(rightTableKey);
                rightTableInfo.setColumnsToRead(columsToReadMap.get(tableKeyId.get(rightTableKey)).toArray(new String[columsToReadMap.get(tableKeyId.get(rightTableKey)).size()]));
                rightTableInfo.setKeyColumnIds(partitionInputs.get(tableKeyId.get(rightTableKey)).getPartitionInfo().getKeyColumnIds());
                rightTableInfo.setStorageInfo(new StorageInfo(Storage.Scheme.s3,null, null, null, null));
                rightTableInfo.setBase(false);

                //TODO: smart parallelism setting
                Integer rightParallelism = tableToInputSplits.get(rightTableKey).size()/numCloudFunction;
                rightTableInfo.setParallelism(rightParallelism);
            
                // put into JoinInput
                joinInput.setSmallTable(leftTableInfo);
                joinInput.setLargeTable(rightTableInfo);
                
                // set PartitionJoinInfo
                joinInfo.setNumPartition(numPartition);
                
                //XXX: why is this 16????
                joinInfo.setHashValues(Arrays.asList(16));

                
                joinInfo.setSmallColumnAlias(joinProjection.get(tableKeyId.get(leftTableKey)).toArray(new String[joinProjection.get(tableKeyId.get(leftTableKey)).size()]));
                joinInfo.setLargeColumnAlias(joinProjection.get(tableKeyId.get(rightTableKey)).toArray(new String[joinProjection.get(tableKeyId.get(rightTableKey)).size()]));
                //XXX: potential problem of post partiotion
                joinInfo.setPostPartition(false);

                //Set projection for each table
                Integer leftTableId = tableKeyId.get(leftTableKey);
                Integer rightTableId = tableKeyId.get(rightTableKey);
                List<String> leftProjCol = joinProjection.get(leftTableId);
                List<String> rightProjCol = joinProjection.get(rightTableId);
                boolean [] leftProj = new boolean[columsToReadMap.get(leftTableId).size()];
                boolean [] rightProj = new boolean[columsToReadMap.get(rightTableId).size()];
                Arrays.fill(leftProj, false);
                Arrays.fill(rightProj, false);

                // set projection for left and right table
                for(int i=0;i<columsToReadMap.get(leftTableId).size();i++){
                    List<String> temp = new ArrayList<String>(columsToReadMap.get(leftTableId));
                    if(leftProjCol.contains(temp.get(i))){
                        leftProj[i] = true;
                    }
                }

                for (int i=0;i<columsToReadMap.get(rightTableId).size();i++){
                    List<String> temp = new ArrayList<String>(columsToReadMap.get(rightTableId));
                    if(rightProjCol.contains(temp.get(i))){
                        rightProj[i] = true;
                    }
                }

                joinInfo.setSmallProjection(leftProj);
                joinInfo.setLargeProjection(rightProj);

                // set PartitionedTableInfo
                joinInput.setJoinInfo(joinInfo);
                joinInput.setOutput(new MultiOutputInfo("s3://jingrong-lambda-test/unit_tests/final_results/",
                                new StorageInfo(Storage.Scheme.s3,null, null, null, null),
                                true, Arrays.asList("partitioned_join_"+leftTableKey+"_"+rightTableKey+"_result"))); // force one file currently
                
                // First we need to slice the input splits into Partition parts
                // XXX: smart setting for the number of partition; maybe different for left and right table;
                Integer leftCfNum = (int)Math.ceil((double)tableToInputSplits.get(leftTableKey).size()/(double)numFilesInEachPartition);
                Integer rightCfNum = (int)Math.ceil((double)tableToInputSplits.get(rightTableKey).size()/(double)numFilesInEachPartition);
                PartitionInput leftPartitionInput = partitionInputs.get(tableKeyId.get(leftTableKey));
                PartitionInput rightPartitionInput = partitionInputs.get(tableKeyId.get(rightTableKey));

                //set future list to collect left and right partition output
                List<CompletableFuture<Output>> futures = new ArrayList<CompletableFuture<Output>>();
                
                //set left and right partition output
                List<String> leftOutputList = new ArrayList<String>();
                List<String> rightOutputList = new ArrayList<String>();

                // seting left inputsplit
                // for(int i=0;i<leftCfNum;i++){
                for(int i=0;i<1;i++){
                    List<InputSplit> leftInputSplit = tableToInputSplits.get(leftTableKey);
                    List <InputSplit> leftPartitionInputList = leftInputSplit.subList(i*numFilesInEachPartition, (i+1)*numFilesInEachPartition > leftInputSplit.size()? leftInputSplit.size():(i+1)*numFilesInEachPartition);
                    leftPartitionInput.getTableInfo().setInputSplits(leftPartitionInputList);
                    leftPartitionInput.setOutput(new OutputInfo(intermediateDirPath+leftTableKey+"_part_" + i,
                                            new StorageInfo(Storage.Scheme.s3, null,null, null, null), true));
                    leftOutputList.add(intermediateDirPath+leftTableKey+"_part_" + i);
                    
                    // local invoke partition test
                    futures.add(invokeLocalPartition(leftPartitionInput));

                    // CompletableFuture<Output> leftOutputFuture = InvokerFactory.Instance()
                    //                 .getInvoker(WorkerType.PARTITION).invoke(leftPartitionInput);
                    // futures.add(leftOutputFuture);
                }

                // seting right inputsplit
                // for(int j=0;j<rightCfNum;j++){
                for(int j=0;j<1;j++){
                    List<InputSplit> rightInputSplit = tableToInputSplits.get(rightTableKey);
                    List <InputSplit> rightPartitionInputList = rightInputSplit.subList(j*numFilesInEachPartition, (j+1)*numFilesInEachPartition > rightInputSplit.size()?rightInputSplit.size():(j+1)*numFilesInEachPartition);
                    rightPartitionInput.getTableInfo().setInputSplits(rightPartitionInputList);
                    rightPartitionInput.setOutput(null);
                    rightPartitionInput.setOutput(new OutputInfo(intermediateDirPath+rightTableKey+"_part_" + j,
                                            new StorageInfo(Storage.Scheme.s3,null, null, null, null), true));
                    rightOutputList.add(intermediateDirPath+rightTableKey+"_part_" + j);

                    // local invoke partition test
                    futures.add(invokeLocalPartition(rightPartitionInput));

                    // CompletableFuture<Output> rightOutputFuture = InvokerFactory.Instance()
                    //                 .getInvoker(WorkerType.PARTITION).invoke(rightPartitionInput);          
                    
                    // futures.add(rightOutputFuture);
                }

                // Collect the futures of the partition result 
                try
                {   
                    for (CompletableFuture future : futures) {
                        try {
                            System.out.println("future.get(): "+future.get());
                        } catch (Exception e) {
                            System.out.println("parttition future.get error");
                            e.printStackTrace();
                            // continue;
                        }
                    }
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
                
                // start join process
                joinInput.getSmallTable().setInputFiles(leftOutputList);
                joinInput.getSmallTable().setParallelism(leftCfNum);
                
                joinInput.getLargeTable().setInputFiles(rightOutputList);
                joinInput.getLargeTable().setParallelism(rightCfNum);
                
                try{
                    Output out=invokeLocalPartitionJoin(joinInput).get();
                    System.out.println("join output: "+ out);
                }catch (Exception e){
                    e.printStackTrace();
                }

                // CompletableFuture<Output> joinOutputFuture = InvokerFactory.Instance()
                //                 .getInvoker(WorkerType.PARTITIONED_JOIN).invoke(joinInput);
                // return joinOutputFuture;
                return null;
            }

            public void setAggregateColumnIds(List<PartialAggregationInfo> aggregationInfos, List<String> columnstoreadArray, HashMap<String, List<Boolean>> scanProjection) {

                for (PartialAggregationInfo aggnode: aggregationInfos){
                    List<Boolean> scanProjectionList = scanProjection.get(Integer.toString(aggregationInfos.indexOf(aggnode)));
                    List<String> aggColToread = new ArrayList<String>();

                    for(int i=0;i<scanProjectionList.size();i++){
                        if(scanProjectionList.get(i)){
                            aggColToread.add(columnstoreadArray.get(i));
                        }
                    }


                    List<String> aggColList = aggnode.getAggregateColumnNames();
                    List<String> groupColList = aggnode.getGroupKeyColumnNames();
                    int [] aggregateColumnIds = new int[aggColList.size()>0?aggColList.size():1];
                    int [] groupKeyColumnIds = new int[groupColList.size()];
                    for(int i=0;i<groupColList.size();i++){
                        groupKeyColumnIds[i] = aggColToread.indexOf(groupColList.get(i));
                    }

                    if (aggColList.size() == 0){
                        aggregateColumnIds[0] = groupKeyColumnIds[0];
                    } else {
                        for(int i=0;i<aggColList.size();i++){
                        
                            aggregateColumnIds[i] = aggColToread.indexOf(aggColList.get(i));
                        }
                    }
                    aggnode.setAggregateColumnIds(aggregateColumnIds);
                    aggnode.setGroupKeyColumnIds(groupKeyColumnIds);
                
                }
            }

            public boolean checkHasOperation(Graph<Integer,DefaultEdge> graph, String operation) {
                boolean hasOperation = false;

                if (operation.equals("LogicalTableScan")) {
                    hasOperation = graph.vertexSet().stream().anyMatch(tableScanNode::contains);
                    return hasOperation;
                }else if (operation.equals("LogicalJoin")||operation.equals("LogicalUnion")) {
                    hasOperation = graph.vertexSet().stream().anyMatch(joinunionNode::contains);
                    return hasOperation;
                }else if (operation.equals("LogicalFilter")) {
                    hasOperation = graph.vertexSet().stream().anyMatch(filterNode::contains);
                    return hasOperation;
                }else if (operation.equals("LogicalProject")) {
                    hasOperation = graph.vertexSet().stream().anyMatch(projectNode::contains);
                    return hasOperation;
                }else if (operation.equals("LogicalAggregate")) {
                    hasOperation = graph.vertexSet().stream().anyMatch(aggregateNode::contains);
                    return hasOperation;
                }
                return hasOperation;
            }

            public CompletableFuture<Output> invokeLocalPartitionJoin(PartitionedJoinInput joinInput) {
                WorkerMetrics workerMetrics = new WorkerMetrics();
                Logger logger = LogManager.getLogger(TestRejsonReader.class);
                WorkerContext workerContext = new WorkerContext(logger, workerMetrics, "123456");
                BasePartitionedJoinWorker baseWorker = new BasePartitionedJoinWorker(workerContext);
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        return baseWorker.process(joinInput);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                });                
            }

            public CompletableFuture<FusionOutput> invokeLocalFusionJoinScan(JoinScanFusionInput joinScanInput){
                WorkerMetrics workerMetrics = new WorkerMetrics();
                Logger logger = LogManager.getLogger(TestRejsonReader.class);
                WorkerContext workerContext = new WorkerContext(logger, workerMetrics, "123456");
                BaseJoinScanFusionWorker baseWorker = new BaseJoinScanFusionWorker(workerContext);
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        return baseWorker.process(joinScanInput);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                });                
            }

            public CompletableFuture<Output> invokeLocalPartition(PartitionInput partitionInput) {
                WorkerMetrics workerMetrics = new WorkerMetrics();
                Logger logger = LogManager.getLogger(TestRejsonReader.class);
                WorkerContext workerContext = new WorkerContext(logger, workerMetrics, "123456");
                BasePartitionWorker baseWorker = new BasePartitionWorker(workerContext);
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        return baseWorker.process(partitionInput);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                });                
            }

            public CompletableFuture<ScanOutput> invokeLocalLambda(ThreadScanInput scaninput) {
                WorkerMetrics workerMetrics = new WorkerMetrics();
                Logger logger = LogManager.getLogger(TestRejsonReader.class);
                WorkerContext workerContext = new WorkerContext(logger, workerMetrics, "123456");
                BaseThreadScanWorker baseWorker = new BaseThreadScanWorker(workerContext);
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        return baseWorker.process(scaninput);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                });                
            }

        }
    }

}   


// public void genSubGraphs(){
//     // Traverse the graph and find the breaker node and delete the edges
//     BreadthFirstIterator<Integer, DefaultEdge> bfi = new BreadthFirstIterator<>(OptDag);
//     HashSet<DefaultEdge> edgesToRemove = new HashSet<>();
//     List<Integer> brekerNode = new ArrayList<Integer>();
//     // find the breaker node and edges to remove
//     while (bfi.hasNext()) {
//         Integer currentVertex = bfi.next();
//         if (relIdToNode.get(currentVertex).path("relOp").asText().equals("LogicalAggregate")) {
//             // System.out.println("find the Logical filter operator");
//             brekerNode.add(currentVertex);
//             Set<DefaultEdge> outgoingEdges = OptDag.outgoingEdgesOf(currentVertex);
//             edgesToRemove.addAll(outgoingEdges);
//         } else if (relIdToNode.get(currentVertex).path("relOp").asText().equals("LogicalUnion") 
//         || relIdToNode.get(currentVertex).path("relOp").asText().equals("LogicalJoin")){
//             // System.out.println("find the Logical Union operator");
            
//             Boolean isBreaker = true;
//             // Get parent of the currentvertex

//             // Set<DefaultEdge> incomEdges = OptDag.incomingEdgesOf(currentVertex);
//             // Integer parentVertex = OptDag.getEdgeSource(incomEdges.iterator().next());
            
//             // if (relIdToNode.get(parentVertex).path("relOp").asText().equals("LogicalTableScan")){
//             //     isBreaker = false;
//             // }
            
//             if (isBreaker){
//                 brekerNode.add(currentVertex);
//                 Set<DefaultEdge> incomingEdges = OptDag.incomingEdgesOf(currentVertex);
//                 edgesToRemove.addAll(incomingEdges);
//             }
//         } else {
//             continue;
//         }
//         // System.out.println(currentVertex);
//     }
    
//     // delete edges
//     System.out.println("breaker node"+ brekerNode);
//     System.out.println("edges to delete" + edgesToRemove);               
//     for (DefaultEdge edge : edgesToRemove) {
//         OptDag.removeEdge(edge);
//     }
    
//     // Seperate the graph vertex into graphlist
//     Set<Integer> vertexSet = new HashSet<Integer>(OptDag.vertexSet());
//     System.out.println("node: "+vertexSet);
//     List<HashSet<Integer>> graphList = new ArrayList<HashSet<Integer>>();
    
//     for(Integer node: brekerNode){
//         if (OptDag.inDegreeOf(node) == 0 && OptDag.outDegreeOf(node)!=0) {
//                 Set<Integer> descSet = OptDag.getDescendants(node);
//                 descSet.add(node);
//                 graphList.add(new HashSet<Integer>(descSet));
//                 vertexSet.removeAll(descSet);

//             } else if (OptDag.outDegreeOf(node) == 0 && OptDag.inDegreeOf(node)!=0) {
//                 Set<Integer> ancSet = OptDag.getAncestors(node);
//                 ancSet.add(node);

//             //  System.out.println("in the middle if condition ancSet: "+ancSet);
//                 Integer rootNodeId = ancSet.iterator().next();
//                 System.out.println("rootNodeId: "+rootNodeId);
//                 if (graphList.size()!=0){
//                     ListIterator<HashSet<Integer>> iterator = graphList.listIterator();
//                     while (iterator.hasNext()) {
//                         HashSet<Integer> graph = iterator.next();
//                         if (graph.contains(rootNodeId)){
//                             System.out.println("local graph: "+graph);
//                             HashSet<Integer> newgraph = new HashSet<Integer>(graph);
//                             newgraph.addAll(ancSet);
//                             iterator.remove();
//                             iterator.add(newgraph);
//                         } else {
//                             iterator.add(new HashSet<Integer>(ancSet));
//                         }
//                         vertexSet.removeAll(ancSet);
//                     }
//                 } else {
//                     graphList.add(new HashSet<Integer>(ancSet));
//                     vertexSet.removeAll(ancSet);
//                     ancSet.forEach(anc -> {
//                         vertexSet.remove(anc);
//                     });

//                 //  System.out.println("deleted");
//                 }
//             } else {
//                 Set<Integer> nodeset= new HashSet<Integer>();
//                 nodeset.add(node);
//                 graphList.add(new HashSet<Integer>(nodeset));
//                 vertexSet.remove(node);
//             }
//     }


//     if (vertexSet.size()!=0){
//         System.out.println("add vertexSet to graphlist" + vertexSet);
//         graphList.add(new HashSet<Integer>(vertexSet));
//     }

//     // Build subgraphs

//     graphList.forEach(graph -> {
//         subGraphs.add(new AsSubgraph<>(OptDag, graph));
//     });


//     System.out.println("cut herer");
//     subGraphs.forEach(subGraph -> {
//         System.out.println("subGraph vertexSet: " + subGraph.vertexSet() + " edgeSet: " + subGraph.edgeSet());
//     });
//     System.out.println("cut herer");
    
//     // System.out.println(relIdToNode.get(8).path("outputPath").asText());

// }

                       