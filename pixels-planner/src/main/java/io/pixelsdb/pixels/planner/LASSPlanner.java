package io.pixelsdb.pixels.planner;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.BreakIterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import com.fasterxml.jackson.databind.node.ObjectNode;
// import com.jayway.jsonpath.internal.function.numeric.Max;

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
import org.jgrapht.traverse.DepthFirstIterator;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
// import org.eclipse.jetty.server.Response.OutputType;
import org.jgrapht.graph.DefaultEdge;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.HashSet;
import org.jgrapht.graph.AsSubgraph;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

import java.util.SortedMap;
import java.util.TreeMap;
import io.pixelsdb.pixels.executor.predicate.ColumnFilter;
import java.util.Map;  
import io.pixelsdb.pixels.executor.predicate.Filter;
import io.pixelsdb.pixels.executor.aggregation.FunctionType;
import io.pixelsdb.pixels.executor.predicate.Bound;

import java.util.concurrent.Future;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.stream.Collectors;
import io.pixelsdb.pixels.common.turbo.Output;
import java.util.Collections;

// import io.pixelsdb.pixels.worker.common.BaseAggregationWorker;
// import io.pixelsdb.pixels.worker.common.BaseJoinScanFusionWorker;
// import io.pixelsdb.pixels.worker.common.BasePartitionWorker;
///for local invoking
// import io.pixelsdb.pixels.worker.common.BaseThreadScanWorker;
// import io.pixelsdb.pixels.worker.common.WorkerContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.units.qual.s;

// import io.pixelsdb.pixels.worker.common.WorkerMetrics;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Iterator;

import io.pixelsdb.pixels.planner.plan.physical.output.AggregationOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.FusionOutput;
import io.pixelsdb.pixels.planner.plan.physical.output.PartitionOutput;
import io.pixelsdb.pixels.executor.join.JoinType;
// import io.pixelsdb.pixels.worker.common.BasePartitionedJoinWorker;
import com.alibaba.fastjson.JSONArray;
import com.google.common.base.Joiner;
//for get operator 
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;
import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.common.exception.InvalidArgumentException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.layout.*;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Ordered;
import io.pixelsdb.pixels.common.metadata.domain.Projections;
import io.pixelsdb.pixels.common.metadata.domain.Splits;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.executor.join.JoinAlgorithm;
import io.pixelsdb.pixels.planner.plan.PlanOptimizer;
import io.pixelsdb.pixels.planner.plan.logical.*;
import io.pixelsdb.pixels.planner.plan.physical.*;
import io.pixelsdb.pixels.planner.plan.physical.domain.*;
//for aggregation
//For join
import io.pixelsdb.pixels.planner.plan.physical.input.*;
import java.util.Optional;
import io.pixelsdb.pixels.planner.plan.logical.calcite.field;


import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;


public class LASSPlanner{

    private JsonNode jsonNode;
    // private List<JsonNode> lambdaRels = new ArrayList<>();
    private ObjectMapper mapper = new ObjectMapper();
    
    // For generateDAG()
    private DirectedAcyclicGraph<Integer,DefaultEdge> OptDag = new DirectedAcyclicGraph<Integer,DefaultEdge>(DefaultEdge.class);
    private HashMap<Integer,ObjectNode> relIdToNode = new HashMap<Integer,ObjectNode>();
    private HashMap<Integer,String> relIdToOperation = new HashMap<Integer,String>();

    // intermediate folder Path
    // private String intermediateDirPath;
    // private String finalDirPath;
    // private String schemaPath;

    // For geneDAg() and optDAG()
    private List<Integer> tableScanNode = new ArrayList<Integer>();
    private List<Integer> joinunionNode = new ArrayList<Integer>();
    private List<Integer> calNode = new ArrayList<Integer>();
    private List<Integer> aggregateNode = new ArrayList<Integer>();

    //For subgraphs
    private List<Graph<Integer,DefaultEdge>> subGraphs = new ArrayList<Graph<Integer,DefaultEdge>>();

    private Map<Integer,Set<Integer>> subgraphList = new HashMap<Integer,Set<Integer>>();

    // for stages
    private Map<Integer,List<List<Integer>>> stages = new HashMap<Integer,List<List<Integer>>>();

    // TODO:For configs, expecially for numFilesInEachPartition
    private int numCloudFunction = 10;
    private int numFilesInEachPartition = 10;
    private int parallelism = 0;
    private int numPartition = 40;


    // for invoke
    private static final Logger logger = LogManager.getLogger(LASSPlanner.class);
    private static final StorageInfo InputStorageInfo;
    private static final StorageInfo IntermediateStorageInfo;
    private static final String IntermediateFolder;
    private static final String finalDirPath;
    private static final int IntraWorkerParallelism;

    private final ConfigFactory config;
    private final MetadataService metadataService;
    private final int fixedSplitSize;
    private final boolean projectionReadEnabled;
    private final boolean orderedPathEnabled;
    private final boolean compactPathEnabled;
    private final Storage storage;
    private final long transId;

    static
    {
        Storage.Scheme inputStorageScheme = Storage.Scheme.from(
                ConfigFactory.Instance().getProperty("executor.input.storage.scheme"));
        InputStorageInfo = StorageInfoBuilder.BuildFromConfig(inputStorageScheme);

        Storage.Scheme interStorageScheme = Storage.Scheme.from(
                ConfigFactory.Instance().getProperty("executor.intermediate.storage.scheme"));
        IntermediateStorageInfo = StorageInfoBuilder.BuildFromConfig(interStorageScheme);
        String interStorageFolder = ConfigFactory.Instance().getProperty("executor.intermediate.folder"); 
        if (!interStorageFolder.endsWith("/"))
        {
            interStorageFolder += "/";
        }
        IntermediateFolder = interStorageFolder;
        finalDirPath = ConfigFactory.Instance().getProperty("executor.output.folder");
        IntraWorkerParallelism = Integer.parseInt(ConfigFactory.Instance()
                .getProperty("executor.intra.worker.parallelism"));
    }


        /**
     * Create an executor for the join plan that is represented as a joined table.
     * In the join plan, all multi-pipeline joins are of small-left endian, all single-pipeline
     * joins have joined table on the left. There is no requirement for the end-point joins of
     * table base tables.
     *
     * @param transId the transaction id
     * @param JsonNode the join plan
     * @param orderedPathEnabled whether ordered path is enabled
     * @param compactPathEnabled whether compact path is enabled
     * @param metadataService the metadata service to access Pixels metadata
     * @throws IOException
     */
    public LASSPlanner(JsonNode jsonNode, long transId, boolean orderedPathEnabled,
                         boolean compactPathEnabled,
                         Optional<MetadataService> metadataService) throws IOException
    {   
        this.jsonNode = jsonNode;
        this.transId = transId;
        this.config = ConfigFactory.Instance();
        // test
        System.out.println(config.getProperty("metadata.server.host"));
        System.out.println(config.getProperty("metadata.server.port"));
        // test
        this.metadataService = metadataService.orElseGet(() ->
                new MetadataService(config.getProperty("metadata.server.host"),
                        Integer.parseInt(config.getProperty("metadata.server.port"))));
        this.fixedSplitSize = Integer.parseInt(config.getProperty("fixed.split.size"));
        this.projectionReadEnabled = Boolean.parseBoolean(config.getProperty("projection.read.enabled"));
        this.orderedPathEnabled = orderedPathEnabled;
        this.compactPathEnabled = compactPathEnabled;
        

        System.out.println(InputStorageInfo.getScheme());
        this.storage = StorageFactory.Instance().getStorage(InputStorageInfo.getScheme());
    }

    public LASSPlanner(Path jsonPath, long transId,
    boolean orderedPathEnabled,
    boolean compactPathEnabled,
    Optional<MetadataService> metadataService) throws IOException
    {   
        this.jsonNode = readAsTree(jsonPath);
        this.transId = transId;
        this.config = ConfigFactory.Instance();
        // test
        System.out.println(config.getProperty("metadata.server.host"));
        System.out.println(config.getProperty("metadata.server.port"));
        // test
        this.metadataService = metadataService.orElseGet(() ->
        new MetadataService(config.getProperty("metadata.server.host"),
        Integer.parseInt(config.getProperty("metadata.server.port"))));
        this.fixedSplitSize = Integer.parseInt(config.getProperty("fixed.split.size"));
        this.projectionReadEnabled = Boolean.parseBoolean(config.getProperty("projection.read.enabled"));
        this.orderedPathEnabled = orderedPathEnabled;
        this.compactPathEnabled = compactPathEnabled;

        System.out.println(InputStorageInfo.getScheme());
        this.storage = StorageFactory.Instance().getStorage(InputStorageInfo.getScheme());
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
                String relOpName = relOp.substring(relOp.lastIndexOf(".")+1);
                Integer last_reiId = Integer.parseInt( last_rel_id.toString());
                System.out.println("relId: "+relId+" relOp: "+relOpName+" last_rel_id: "+last_reiId);
                relIdToNode.put(relId,(ObjectNode) rel);
                relIdToOperation.put(relId,relOpName);

                // for each operation
                if (relOp.contains("EnumerableTableScan")) {
                    OptDag.addVertex(relId);
                    tableScanNode.add(relId);
                } else if (relOp.contains("EnumerableCalc")){
                    //EnumerableCalc contains project and filter
                    OptDag.addVertex(relId);
                    calNode.add(relId);
                    if(rel.path("inputs").size()!=0){
                        OptDag.addEdge(relId,Integer.parseInt(rel.path("inputs").get(0).asText()));
                    }else{
                        OptDag.addEdge(relId, last_reiId);
                    }

                }else if (relOp.contains("EnumerableHashJoin")||relOp.contains("EnumerableUnion")) {
                    try {
                        joinunionNode.add(relId);
                        //TODO:POTENTIAL PROBLEM OF MAPPER TO LIST
                        List<String> joinInputs = mapper.readerForListOf(String.class).readValue(rel.path("inputs"));
                        OptDag.addVertex(relId);
                        joinInputs.forEach(joinInput -> {
                            Integer joinInputId = Integer.parseInt(joinInput);
                            OptDag.addEdge(relId,joinInputId);
                        });
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                } else if (relOp.contains("EnumerableAggregate")) {
                    if(rel.path("inputs").size()!=0){
                        OptDag.addVertex(relId);
                        OptDag.addEdge(relId,Integer.parseInt(rel.path("inputs").get(0).asText()));
                    }else{
                        OptDag.addVertex(relId);
                        OptDag.addEdge(relId, last_reiId);
                    }
                } else {

                    OptDag.addVertex(relId);
                    OptDag.addEdge(relId, last_reiId);
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

    public void setInputOutputPath() {
        System.out.println("Start set Input and Output Path for optDAG");
        System.out.println("Vertext sets: " + OptDag.vertexSet()+"\n"+"Edge sets: "+OptDag.edgeSet());
        // TODO: set input output path for tablescan

    }

    public List<Integer> findbreakerNode(){
        // Traverse the graph and find the breaker node
        // BreadthFirstIterator<Integer, DefaultEdge> bfi = new BreadthFirstIterator<>(OptDag);
        List<Integer> breakerNode = new ArrayList<Integer>();
        DepthFirstIterator<Integer, DefaultEdge> bfi = new DepthFirstIterator<>(OptDag,Collections.max(OptDag.vertexSet()));

        // find the breaker node and edges to remove
        Integer lastVertex = null;
        while (bfi.hasNext()) {
            //add the first node
            Integer currentVertex = bfi.next();
            
            if (
            relIdToNode.get(currentVertex).path("relOp").asText().contains("EnumerableUnion") 
            || relIdToNode.get(currentVertex).path("relOp").asText().contains("EnumerableHashJoin")) {
                //add to breaker node
                breakerNode.add(currentVertex);
            } //if next node is limit, then limit is the breaker node
            else if(relIdToNode.get(currentVertex).path("relOp").asText().contains("EnumerableLimit") && bfi.hasNext()){
                Integer nextVertex = bfi.next();
                if(relIdToNode.get(nextVertex).path("relOp").asText().contains("EnumerableSort")){
                    breakerNode.add(currentVertex);
                }
            } else if(relIdToNode.get(currentVertex).path("relOp").asText().contains("EnumerableSort") && relIdToNode.get(lastVertex).path("relOp").asText().contains("EnumerableLimit")){
                breakerNode.add(currentVertex);
            } else if(relIdToNode.get(currentVertex).path("relOp").asText().contains("EnumerableAggregate")){
                if(relIdToNode.get(currentVertex).path("aggs").size()!=0){
                    breakerNode.add(currentVertex);
                }
            }else {
                continue;
            }

            lastVertex = currentVertex;

            
        }

        return breakerNode;
    }

    public Set<Integer> dfsTraversal(Graph<Integer, DefaultEdge> graph, Integer startNode, Integer targetNode) {
        Set<Integer> visited = new HashSet<Integer>();
        Stack<Integer> stack = new Stack<>();
        stack.push(startNode);

        while (!stack.isEmpty()) {
            Integer current = stack.pop();
            visited.add(current);
            // System.out.println("Visiting node: " + current);

            if (current.equals(targetNode)) {
                System.out.println("Reached target node: " + targetNode);
                visited.add(targetNode);
                break; // 停止遍历
            }

            for (DefaultEdge edge : graph.outgoingEdgesOf(current)) {
                Integer neighbor = Graphs.getOppositeVertex(graph, edge, current);
                if (!neighbor.equals(startNode)) { // 避免回到起始节点
                    stack.push(neighbor);
                    visited.add(neighbor);
                }
            }
        }
        return visited;
    }
    
    public Set<Integer> bfsTraversal(Graph<Integer, DefaultEdge> graph, int startNode, int stopNode) {
        Queue<Integer> queue = new LinkedList<>();
        Set<Integer> visited = new HashSet<>();

        queue.add(startNode);
        while (!queue.isEmpty()) {
            int current = queue.poll();
            visited.add(current);
            
            if (current != stopNode) {
                for (DefaultEdge edge : graph.outgoingEdgesOf(current)) {
                    int neighbor = Graphs.getOppositeVertex(graph, edge, current);
                    if (!visited.contains(neighbor)) {
                        queue.add(neighbor);
                        visited.add(neighbor);
                    }
                }
            }

            if(current != startNode){
                for (DefaultEdge edge : graph.incomingEdgesOf(current)){
                    int neighbor = Graphs.getOppositeVertex(graph, edge, current);
                    
                    if (!visited.contains(neighbor)) {
                        queue.add(neighbor);
                        visited.add(neighbor);
                    }

                }
            }

            if (current == stopNode && !queue.isEmpty()) {
                while (!queue.isEmpty()){
                    int temp = queue.poll();
                    if(temp != startNode){
                        for (DefaultEdge edge : graph.incomingEdgesOf(temp)){
                            int neighbor = Graphs.getOppositeVertex(graph, edge, temp);
                            
                            if (!visited.contains(neighbor)) {
                                queue.add(neighbor);
                                visited.add(neighbor);
                            }
    
                        }
                    }
                }
                
            }

            if (current == stopNode && queue.isEmpty()) {
                break; 
            }

        }
        

        return visited;
    }
    
    public boolean containsBreakerNode(Set<Integer> subGraphNodes, List<Integer> breakerNode){
        boolean containsBreakerNode = false;
        //remove first and last element
        Set<Integer> localSubGraphNodes = new HashSet<Integer>(subGraphNodes);

        localSubGraphNodes.remove(Collections.max(subGraphNodes));
        localSubGraphNodes.remove(Collections.min(subGraphNodes));

        for (Integer node : localSubGraphNodes){
            if (breakerNode.contains(node)){
                containsBreakerNode = true;
            }
        }
        return containsBreakerNode;
    }

    public Set<Integer> commonNode(Set<Integer> subGraphNodes, Set<Integer> breakerNode){
        Set<Integer> commonNode = new HashSet<Integer>();
        //remove first and last element
        Set<Integer> localSubGraphNodes = new HashSet<Integer>(subGraphNodes);

        localSubGraphNodes.remove(Collections.max(subGraphNodes));
        localSubGraphNodes.remove(Collections.min(subGraphNodes));

        for (Integer node : localSubGraphNodes){
            if (breakerNode.contains(node)){
                commonNode.add(node);
            }
        }
        return commonNode;
    }

    public void genSubGraphs(){
        System.out.println("Vertext sets: "+OptDag.vertexSet()+"\n"+"Edge sets: "+OptDag.edgeSet());
        List<Graph<String, DefaultEdge>> graphList = new ArrayList<>();
        List<Integer> breakerNode = findbreakerNode();
        breakerNode.add(0);
        System.out.println("breakernode after adding head :" + breakerNode);
        Set<Integer> breakernodeSets = new HashSet<Integer>(breakerNode);

        for (int i=0;i<breakerNode.size();i++){ 
            if((i+1<breakerNode.size()) && (breakerNode.get(i) > breakerNode.get(i+1)) ){
                Set<Integer> subGraphNodes = dfsTraversal(OptDag,breakerNode.get(i), breakerNode.get(i+1));
                System.out.println("process node:"+breakerNode.get(i)+" to "+ breakerNode.get(i+1));

                if(containsBreakerNode(subGraphNodes,breakerNode) && !commonNode(subGraphNodes,breakernodeSets).contains(breakerNode.get(i+1))){
                    System.out.println("contains breakernode, remove the nodes: "+commonNode(subGraphNodes,breakernodeSets));
                    subGraphNodes.removeAll(commonNode(subGraphNodes,breakernodeSets));
                }

            
                if (relIdToOperation.get(breakerNode.get(i)).equals("EnumerableHashJoin") ){
                    //add inputs node into subgraph
                    for(int j = 0; j<relIdToNode.get(breakerNode.get(i)).path("inputs").size();j++){
                        Integer inputNode = Integer.parseInt(relIdToNode.get(breakerNode.get(i)).path("inputs").get(j).asText());
                        subGraphNodes.add(inputNode);
                        if(relIdToNode.get(inputNode).path("inputs").size()!=0 && relIdToOperation.get(inputNode).equals("EnumerableCalc")){

                            Integer tmpNode = Integer.parseInt(relIdToNode.get(inputNode).path("inputs").get(0).asText());
                            System.out.println("contains upstream inputs field, add node: "+tmpNode);
                            subGraphNodes.add(tmpNode);
                        }

                    }

                }
                
                System.out.println("subGraphNodes: " + subGraphNodes);
                subgraphList.put(breakerNode.get(i),subGraphNodes);
            }
        }

        // put the rest of the node into one subgraph, beacause sometime they are not in breaker node;
        Set<Integer> restNodes = new HashSet<Integer>();
        for (Integer node : OptDag.vertexSet()){
            if (!flattenNestedMapOfSets(subgraphList).contains(node)){
                restNodes.add(node);
            }
        }
        System.out.println("restNodes :"+restNodes);

        // for(Integer node : restNodes){
            
        //     Set<Integer> subGraphNodes = new HashSet<Integer>();

        //     if(relIdToNode.get(node).path("inputs").size()!=0){
        //         Integer startnode = Integer.parseInt(relIdToNode.get(node).path("inputs").get(0).asText());
        //         subGraphNodes.add(startnode);
        //         subGraphNodes.add(node);
        //         Integer endnode = Graphs.getOppositeVertex(OptDag, OptDag.outgoingEdgesOf(node).iterator().next(), node);
        //         subGraphNodes.add(endnode);
        //         subgraphList.put(startnode,subGraphNodes);
        //     }


        // }


        System.out.println("subgraphList: " + subgraphList);

    }

    public void genStages(){
        //put subgraphs into stages
        //sort the subgraphList
        Map<Integer,Set<Integer>> sortedSubgraphList = new TreeMap<Integer,Set<Integer>>(subgraphList);
        System.out.println("sortedSubgraphList: " + sortedSubgraphList);

        int stageNum = 0;
        Map<Integer,Integer> headToStage = new HashMap<Integer,Integer>();
        Map<Integer,Integer> tailTostage = new HashMap<Integer,Integer>();
        Map<List<Integer>,Integer> middlesToStage = new HashMap<List<Integer>,Integer>();

        for (Map.Entry<Integer, Set<Integer>> entry : sortedSubgraphList.entrySet()){
            List<Integer> temp = entry.getValue().stream().collect(Collectors.toList());
            temp.remove(Collections.max(entry.getValue()));
            temp.remove(Collections.min(entry.getValue()));

            if (!stages.containsKey(stageNum)){
                stages.put(stageNum, Arrays.asList(new ArrayList<>(entry.getValue())));
                tailTostage.put(Collections.max(entry.getValue()), stageNum);
                headToStage.put(Collections.min(entry.getValue()), stageNum);
                middlesToStage.put(temp, stageNum);
                continue;
            } else{
                boolean isStage = false; 
                boolean isInMiddleOfStage = false;
                
                //get head of the subgraph
                Integer head = Collections.min(entry.getValue());
                //get tail of the subgraph
                Integer tail = Collections.max(entry.getValue());

                if (tailTostage.containsKey(head)){
                    stageNum = tailTostage.get(head);
                    isStage = true;
                }

                if (isStage){
                    stageNum++;
                    stages.put(stageNum, Arrays.asList(new ArrayList<>(entry.getValue())));
                    tailTostage.put(Collections.max(entry.getValue()), stageNum);
                    headToStage.put(Collections.min(entry.getValue()), stageNum);
                    middlesToStage.put(temp, stageNum);
                }

                if(headToStage.containsKey(head) && !isStage){
                    stageNum = headToStage.get(head);
                    List<List<Integer>> tempStage = new ArrayList<>(stages.get(stageNum));
                    tempStage.add(new ArrayList<>(entry.getValue()));
                    stages.put(stageNum, tempStage);
                    tailTostage.put(Collections.max(entry.getValue()), stageNum);
                    headToStage.put(Collections.min(entry.getValue()), stageNum);
                    middlesToStage.put(temp, stageNum);
                }


                for (List<Integer> middles: middlesToStage.keySet()){
                    if (middles.contains(head)){
                        stageNum = middlesToStage.get(middles);
                        isInMiddleOfStage = true;
                    }
                }

                if (isInMiddleOfStage){
                    List<List<Integer>> tempStage = new ArrayList<>(stages.get(stageNum));
                    tempStage.add(new ArrayList<>(entry.getValue()));
                    stages.put(stageNum, tempStage);
                    tailTostage.put(Collections.max(entry.getValue()), stageNum);
                }


            }
        }
        System.out.println("stages with nodes only: " + stages);
        
        Set<Integer> temp=flattenNestedMapOfSets(subgraphList);
        temp.removeAll(flattenStages(stages));

        System.out.println("nodes not cover yet: "+ temp);
    }
    
    public Set<Integer> flattenStages(Map<Integer,List<List<Integer>>> stages) {
        Set<Integer> flattenedSet = new HashSet<>();

        for (List<List<Integer>> subset : stages.values()) {
            for (List<Integer> subsubset : subset){
                flattenedSet.addAll(subsubset);
            }
        }

        return flattenedSet;
    }

    public JoinedTable generateJoinedTable(List<Integer> subgraph){
        
        // BaseTable left = new BaseTable(
        //         "tpch", "region", "region",
        //         new String[] {"r_regionkey", "r_name"},
        //         TableScanFilter.empty("tpch", "region"));

        // BaseTable right = new BaseTable(
        //             "tpch", "region", "region",
        //             new String[] {"r_regionkey", "r_name"},
        //             TableScanFilter.empty("tpch", "region"));
        
        boolean left=false;
        boolean right=false;

        field[] l_columFileds = null;
        field[] r_columFileds = null;

        String l_schemaName = null;
        String l_tableName = null;
        String l_alias = null;
        String[] l_columnNames = null;
        TableScanFilter l_filter = null;

        String r_schemaName = null;
        String r_tableName = null;
        String r_alias = null;
        String[] r_columnNames = null;
        TableScanFilter r_filter = null;

        Integer targetNode = Collections.min(subgraph);
        Integer startNode = Collections.max(subgraph);
        Set<Integer> visited = new HashSet<Integer>();
        Stack<Integer> stack = new Stack<>();
        stack.push(startNode);

        System.out.println("Start assemble: ");
        try{

        
        while (!stack.isEmpty()) {
            Integer current = stack.pop();
            visited.add(current);
            ObjectNode node = relIdToNode.get(current);

            System.out.println("Visiting node: " + current);
            if (relIdToOperation.get(current).equals("EnumerableTableScan")) {
                if(!left){
                    l_schemaName = node.path("table").get(0).asText();
                    l_tableName = node.path("table").get(1).asText();
                    l_alias = l_tableName;
                }else if(!right){
                    r_schemaName = node.path("table").get(0).asText();
                    r_tableName = node.path("table").get(1).asText();
                    r_alias = r_tableName;
                } else {
                    System.out.println("Error: more than two EnumerableTableScan");
                }
            } else if (relIdToOperation.get(current).equals("EnumerableCalc")){
                //EnumerableCalc contains project and filter
                if(!left){
                    System.out.println("fileds"+node.path("outputRowType").path("fields"));
                    l_columFileds = mapper.readerFor(field[].class).readValue(node.path("outputRowType").path("fields"));
                    List<String> temp = new ArrayList<String>();

                    for (field field : l_columFileds){
                        temp.add(field.getName());
                    }
                    l_columnNames = temp.toArray(new String[temp.size()]);

                    // System.out.println(node.path("condition"));
                    if(node.path("condition").asText()=="null"){
                        l_filter = TableScanFilter.empty(l_schemaName, l_tableName);
                    }

                    left = true;
                }else if(!right){
                    r_columFileds = mapper.readerFor(field[].class).readValue(node.path("outputRowType").path("fields"));
                    List<String> temp = new ArrayList<String>();

                    for (field field : r_columFileds){
                        temp.add(field.getName());
                    }
                    r_columnNames = temp.toArray(new String[temp.size()]);

                    // System.out.println(node.path("condition"));
                    if(node.path("condition").asText()=="null"){
                        r_filter = TableScanFilter.empty(r_schemaName, r_tableName);
                    }

                    right = true;
                } else {
                    System.out.println("Error: more than two EnumerableCalc");

                } 
            }

            BaseTable leftTable = new BaseTable(
                l_schemaName, l_tableName, l_alias,
                l_columnNames,
                l_filter);
        
            BaseTable rightTable = new BaseTable(
                r_schemaName, r_tableName, r_alias,
                r_columnNames,
                r_filter);

            if (relIdToOperation.get(current).equals("EnumerableHashJoin")){
                


            }
            






            if (current.equals(targetNode)) {
                System.out.println("Reached target node: " + targetNode);
                // visited.add(targetNode);
                break; // 停止遍历
            }

            for (DefaultEdge edge : OptDag.outgoingEdgesOf(current)) {
                Integer neighbor = Graphs.getOppositeVertex(OptDag, edge, current);
                if (!neighbor.equals(startNode)) { // 避免回到起始节点
                    stack.push(neighbor);
                    // visited.add(neighbor);
                }
            }
        }
        
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public void invoke(){

        for (Map.Entry<Integer, List<List<Integer>>> entry : stages.entrySet()){
            Integer stageNum = entry.getKey();
            System.out.println("Start stageNum: " + stageNum);
            List<List<Integer>> stageValues = entry.getValue();
            System.out.println(stageValues.size());

            if(stageValues.size()==1){
                System.out.println("Only one subgraph in this stage");
                List<Integer> subgraph = stageValues.get(0);
                Integer maxNode = Collections.max(subgraph);
                System.out.println("max node is " + maxNode + " max node operation is " + relIdToOperation.get(maxNode));
                
                if (relIdToOperation.get(maxNode).equals("EnumerableHashJoin")){
                    System.out.print("HashJoin node");

                    JoinedTable joinedTable = generateJoinedTable(subgraph);
                    // try{
                    //     JoinOperator joinOperator = getJoinOperator(joinedTable, Optional.empty());
                    // }catch (Exception e){
                    //     e.printStackTrace();
                    // }
                    
                    
                }


                


            } else {
                System.out.println("More than one subgraph in this stage");

                for (List<Integer> subgraph : stageValues){
                    System.out.println("subgraph: " + subgraph);
                }

            }




            System.out.println("stage: " + stageValues);
        }

    }

    private ProjectionsIndex buildProjectionsIndex(Ordered ordered, Projections projections, SchemaTableName schemaTableName)
    {
        List<String> columnOrder = ordered.getColumnOrder();
        ProjectionsIndex index;
        index = new InvertedProjectionsIndex(columnOrder, ProjectionPattern.buildPatterns(columnOrder, projections));
        IndexFactory.Instance().cacheProjectionsIndex(schemaTableName, index);
        return index;
    }

    private BroadcastTableInfo getBroadcastTableInfo(
            Table table, List<InputSplit> inputSplits, int[] keyColumnIds)
    {
        BroadcastTableInfo tableInfo = new BroadcastTableInfo();
        tableInfo.setTableName(table.getTableName());
        tableInfo.setInputSplits(inputSplits);
        tableInfo.setColumnsToRead(table.getColumnNames());
        if (table.getTableType() == Table.TableType.BASE)
        {
            tableInfo.setFilter(JSON.toJSONString(((BaseTable) table).getFilter()));
            tableInfo.setBase(true);
            tableInfo.setStorageInfo(InputStorageInfo);
        }
        else
        {
            tableInfo.setFilter(JSON.toJSONString(
                    TableScanFilter.empty(table.getSchemaName(), table.getTableName())));
            tableInfo.setBase(false);
            tableInfo.setStorageInfo(IntermediateStorageInfo);
        }
        tableInfo.setKeyColumnIds(keyColumnIds);
        return tableInfo;
    }

        /**
     * Get the join inputs of a partitioned join, given the left and right partitioned tables.
     * @param joinedTable this joined table
     * @param parent the parent of this joined table
     * @param numPartition the number of partitions
     * @param leftTableInfo the left partitioned table
     * @param rightTableInfo the right partitioned table
     * @param leftPartitionProjection the partition projection of the left table, null if not exists
     * @param rightPartitionProjection the partition projection of the right table, null if not exists
     * @return the join input of this join
     * @throws MetadataException
     */
    private List<JoinInput> getPartitionedJoinInputs(
            JoinedTable joinedTable, Optional<JoinedTable> parent, int numPartition,
            PartitionedTableInfo leftTableInfo, PartitionedTableInfo rightTableInfo,
            boolean[] leftPartitionProjection, boolean[] rightPartitionProjection)
            throws MetadataException, InvalidProtocolBufferException
    {
        boolean postPartition = false;
        PartitionInfo postPartitionInfo = null;
        if (parent.isPresent() && parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.PARTITIONED)
        {
            postPartition = true;
            // Note: DO NOT use numPartition as the number of partitions for post partitioning.
            int numPostPartition = PlanOptimizer.Instance().getJoinNumPartition(
                    this.transId,
                    parent.get().getJoin().getLeftTable(),
                    parent.get().getJoin().getRightTable(),
                    parent.get().getJoin().getJoinEndian());

            // Check if the current table if the left child or the right child of parent.
            if (joinedTable == parent.get().getJoin().getLeftTable())
            {
                postPartitionInfo = new PartitionInfo(parent.get().getJoin().getLeftKeyColumnIds(), numPostPartition);
            }
            else
            {
                postPartitionInfo = new PartitionInfo(parent.get().getJoin().getRightKeyColumnIds(), numPostPartition);
            }
        }

        ImmutableList.Builder<JoinInput> joinInputs = ImmutableList.builder();
        for (int i = 0; i < numPartition; ++i)
        {
            ImmutableList.Builder<String> outputFileNames = ImmutableList
                    .builderWithExpectedSize(IntraWorkerParallelism);
            outputFileNames.add(i + "/join");
            if (joinedTable.getJoin().getJoinType() == JoinType.EQUI_LEFT ||
                    joinedTable.getJoin().getJoinType() == JoinType.EQUI_FULL)
            {
                outputFileNames.add(i + "/join_left");
            }

            String path = IntermediateFolder + transId + "/" + joinedTable.getSchemaName() + "/" +
                    joinedTable.getTableName() + "/";
            MultiOutputInfo output = new MultiOutputInfo(path, IntermediateStorageInfo, true, outputFileNames.build());

            boolean[] leftProjection = leftPartitionProjection == null ? joinedTable.getJoin().getLeftProjection() :
                    rewriteProjectionForPartitionedJoin(joinedTable.getJoin().getLeftProjection(), leftPartitionProjection);
            boolean[] rightProjection = rightPartitionProjection == null ? joinedTable.getJoin().getRightProjection() :
                    rewriteProjectionForPartitionedJoin(joinedTable.getJoin().getRightProjection(), rightPartitionProjection);

            PartitionedJoinInput joinInput;
            if (joinedTable.getJoin().getJoinEndian() == JoinEndian.SMALL_LEFT)
            {
                PartitionedJoinInfo joinInfo = new PartitionedJoinInfo(joinedTable.getJoin().getJoinType(),
                        joinedTable.getJoin().getLeftColumnAlias(), joinedTable.getJoin().getRightColumnAlias(),
                        leftProjection, rightProjection, postPartition, postPartitionInfo, numPartition, ImmutableList.of(i));
                 joinInput = new PartitionedJoinInput(transId, leftTableInfo, rightTableInfo, joinInfo,
                         false, null, output);
            }
            else
            {
                PartitionedJoinInfo joinInfo = new PartitionedJoinInfo(joinedTable.getJoin().getJoinType().flip(),
                        joinedTable.getJoin().getRightColumnAlias(), joinedTable.getJoin().getLeftColumnAlias(),
                        rightProjection, leftProjection, postPartition, postPartitionInfo, numPartition, ImmutableList.of(i));
                joinInput = new PartitionedJoinInput(transId, rightTableInfo, leftTableInfo, joinInfo,
                        false, null, output);
            }

            joinInputs.add(joinInput);
        }
        return joinInputs.build();
    }
        /**
     * Get the partition projection for the base table that is to be partitioned.
     * If a column only exists in the filters but does not exist in the join projection
     * (corresponding element is false), then the corresponding element in the partition
     * projection is false. Otherwise, it is true.
     *
     * @param table the base table
     * @param joinProjection the join projection
     * @return the partition projection for the partition operator
     */
    private boolean[] getPartitionProjection(Table table, boolean[] joinProjection)
    {
        String[] columnsToRead = table.getColumnNames();
        boolean[] projection = new boolean[columnsToRead.length];
        if (table.getTableType() == Table.TableType.BASE)
        {
            TableScanFilter tableScanFilter = ((BaseTable)table).getFilter();
            Set<Integer> filterColumnIds = tableScanFilter.getColumnFilters().keySet();
            for (int i = 0; i < columnsToRead.length; ++i)
            {
                if (!joinProjection[i] && filterColumnIds.contains(i))
                {
                    checkArgument(columnsToRead[i].equals(tableScanFilter.getColumnFilter(i).getColumnName()),
                            "the column name in the table and the filter do not match");
                    projection[i] = false;
                } else
                {
                    projection[i] = true;
                }
            }
        }
        else
        {
            Arrays.fill(projection, true);
        }
        return projection;
    }

    private boolean[] rewriteProjectionForPartitionedJoin(boolean[] originProjection, boolean[] partitionProjection)
    {
        requireNonNull(originProjection, "originProjection is null");
        requireNonNull(partitionProjection, "partitionProjection is null");
        checkArgument(originProjection.length == partitionProjection.length,
                "originProjection and partitionProjection are not of the same length");
        int len = 0;
        for (boolean b : partitionProjection)
        {
            if (b)
            {
                len++;
            }
        }
        if (len == partitionProjection.length)
        {
            return originProjection;
        }

        boolean[] projection = new boolean[len];
        for (int i = 0, j = 0; i < partitionProjection.length; ++i)
        {
            if (partitionProjection[i])
            {
                projection[j++] = originProjection[i];
            }
        }
        return projection;
    }

    /**
     * Get the input splits for a broadcast join from the join inputs of the child.
     * @param joinInputs the join inputs of the child
     * @return the input splits for the broadcast join
     */
    private List<InputSplit> getBroadcastInputSplits(List<JoinInput> joinInputs)
    {
        ImmutableList.Builder<InputSplit> inputSplits = ImmutableList.builder();
        for (JoinInput joinInput : joinInputs)
        {
            MultiOutputInfo output = joinInput.getOutput();
            String base = output.getPath();
            if (!base.endsWith("/"))
            {
                base += "/";
            }
            /*
            * We make each input file as an input split, thus the number of broadcast join workers
            * will be the same as the number of workers of the child join. This provides better
            * parallelism and is required by partitioned chain join.
            * */
            for (String fileName : output.getFileNames())
            {
                InputInfo inputInfo = new InputInfo(base + fileName, 0, -1);
                inputSplits.add(new InputSplit(ImmutableList.of(inputInfo)));
            }
        }
        return inputSplits.build();
    }
    /**
     * Adjust the input splits for a broadcast join, if the output of this join is read by its parent.
     * The adjustment retries to reduce the number of workers in this join according to the selectivity
     * of the small table in this join.
     * @param smallTable the small table in this join
     * @param largeTable the large table in this join
     * @param largeInputSplits the input splits for the large table
     * @return the adjusted input splits for the large table, i.e., this join
     * @throws InvalidProtocolBufferException
     * @throws MetadataException
     */
    private List<InputSplit> adjustInputSplitsForBroadcastJoin(
            Table smallTable, Table largeTable, List<InputSplit> largeInputSplits)
            throws InvalidProtocolBufferException, MetadataException
    {
        int numWorkers = largeInputSplits.size() / IntraWorkerParallelism;
        if (numWorkers <= 32)
        {
            // There are less than 32 workers, they are not likely to affect the performance.
            return largeInputSplits;
        }
        double smallSelectivity = PlanOptimizer.Instance().getTableSelectivity(this.transId, smallTable);
        double largeSelectivity = PlanOptimizer.Instance().getTableSelectivity(this.transId, largeTable);
        if (smallSelectivity >= 0 && largeSelectivity > 0 && smallSelectivity < largeSelectivity)
        {
            // Adjust the input split size if the small table has a lower selectivity.
            int numSplits = largeInputSplits.size();
            int numInputInfos = 0;
            ImmutableList.Builder<InputInfo> inputInfosBuilder = ImmutableList.builder();
            for (InputSplit inputSplit : largeInputSplits)
            {
                numInputInfos += inputSplit.getInputInfos().size();
                inputInfosBuilder.addAll(inputSplit.getInputInfos());
            }
            int inputInfosPerSplit = numInputInfos / numSplits;
            if (numInputInfos % numSplits > 0)
            {
                inputInfosPerSplit++;
            }
            if (smallSelectivity / largeSelectivity < 0.25)
            {
                // Do not adjust too aggressively.
                logger.debug("increasing the split size of table '" + largeTable.getTableName() +
                        "' by factor of 2");
                inputInfosPerSplit *= 2;
            }
            else
            {
                return largeInputSplits;
            }
            List<InputInfo> inputInfos = inputInfosBuilder.build();
            ImmutableList.Builder<InputSplit> inputSplitsBuilder = ImmutableList.builder();
            for (int i = 0; i < numInputInfos; )
            {
                ImmutableList.Builder<InputInfo> builder = ImmutableList.builderWithExpectedSize(inputInfosPerSplit);
                for (int j = 0; j < inputInfosPerSplit && i < numInputInfos; ++j, ++i)
                {
                    builder.add(inputInfos.get(i));
                }
                inputSplitsBuilder.add(new InputSplit(builder.build()));
            }
            return inputSplitsBuilder.build();
        }
        else
        {
            return largeInputSplits;
        }
    }

    /**
     * Get the partitioned file paths from the join inputs of the child.
     * @param joinInputs the join inputs of the child
     * @return the paths of the partitioned files
     */
    private List<String> getPartitionedFiles(List<JoinInput> joinInputs)
    {
        ImmutableList.Builder<String> partitionedFiles = ImmutableList.builder();
        for (JoinInput joinInput : joinInputs)
        {
            MultiOutputInfo output = joinInput.getOutput();
            String base = output.getPath();
            if (!base.endsWith("/"))
            {
                base += "/";
            }
            for (String fileName : output.getFileNames())
            {
                partitionedFiles.add(base + fileName);
            }
        }
        return partitionedFiles.build();
    }

    private String[] rewriteColumnsToReadForPartitionedJoin(String[] originColumnsToRead, boolean[] partitionProjection)
    {
        requireNonNull(originColumnsToRead, "originColumnsToRead is null");
        requireNonNull(partitionProjection, "partitionProjection is null");
        checkArgument(originColumnsToRead.length == partitionProjection.length,
                "originColumnsToRead and partitionProjection are not of the same length");
        int len = 0;
        for (boolean b : partitionProjection)
        {
            if (b)
            {
                len++;
            }
        }
        if (len == partitionProjection.length)
        {
            return originColumnsToRead;
        }

        String[] columnsToRead = new String[len];
        for (int i = 0, j = 0; i < partitionProjection.length; ++i)
        {
            if (partitionProjection[i])
            {
                columnsToRead[j++] = originColumnsToRead[i];
            }
        }
        return columnsToRead;
    }

    private int[] rewriteColumnIdsForPartitionedJoin(int[] originColumnIds, boolean[] partitionProjection)
    {
        requireNonNull(originColumnIds, "originProjection is null");
        requireNonNull(partitionProjection, "partitionProjection is null");
        checkArgument(originColumnIds.length <= partitionProjection.length,
                "originColumnIds has more elements than partitionProjection");
        Map<Integer, Integer> columnIdMap = new HashMap<>();
        for (int i = 0, j = 0; i < partitionProjection.length; ++i)
        {
            if (partitionProjection[i])
            {
                columnIdMap.put(i, j++);
            }
        }
        if (columnIdMap.size() == partitionProjection.length)
        {
            return originColumnIds;
        }

        int[] columnIds = new int[originColumnIds.length];
        for (int i = 0; i < originColumnIds.length; ++i)
        {
            columnIds[i] = columnIdMap.get(originColumnIds[i]);
        }
        return columnIds;
    }

    private PartitionedTableInfo getPartitionedTableInfo(
            Table table, int[] keyColumnIds, List<PartitionInput> partitionInputs, boolean[] partitionProjection)
    {
        ImmutableList.Builder<String> rightPartitionedFiles = ImmutableList.builder();
        for (PartitionInput partitionInput : partitionInputs)
        {
            rightPartitionedFiles.add(partitionInput.getOutput().getPath());
        }

        int[] newKeyColumnIds = rewriteColumnIdsForPartitionedJoin(keyColumnIds, partitionProjection);
        String[] newColumnsToRead = rewriteColumnsToReadForPartitionedJoin(table.getColumnNames(), partitionProjection);

        if (table.getTableType() == Table.TableType.BASE)
        {
            return new PartitionedTableInfo(table.getTableName(), true,
                    newColumnsToRead, InputStorageInfo, rightPartitionedFiles.build(),
                    IntraWorkerParallelism, newKeyColumnIds);
        } else
        {
            return new PartitionedTableInfo(table.getTableName(), false,
                    newColumnsToRead, IntermediateStorageInfo, rightPartitionedFiles.build(),
                    IntraWorkerParallelism, newKeyColumnIds);
        }
    }

    private List<PartitionInput> getPartitionInputs(Table inputTable, List<InputSplit> inputSplits, int[] keyColumnIds,
                                                    boolean[] partitionProjection, int numPartition, String outputBase)
    {
        ImmutableList.Builder<PartitionInput> partitionInputsBuilder = ImmutableList.builder();
        int outputId = 0;
        for (int i = 0; i < inputSplits.size();)
        {
            PartitionInput partitionInput = new PartitionInput();
            partitionInput.setTransId(transId);
            ScanTableInfo tableInfo = new ScanTableInfo();
            ImmutableList.Builder<InputSplit> inputsBuilder = ImmutableList
                    .builderWithExpectedSize(IntraWorkerParallelism);
            for (int j = 0; j < IntraWorkerParallelism && i < inputSplits.size(); ++j, ++i)
            {
                // We assign a number of IntraWorkerParallelism input-splits to each partition worker.
                inputsBuilder.add(inputSplits.get(i));
            }
            tableInfo.setInputSplits(inputsBuilder.build());
            tableInfo.setColumnsToRead(inputTable.getColumnNames());
            tableInfo.setTableName(inputTable.getTableName());
            if (inputTable.getTableType() == Table.TableType.BASE)
            {
                tableInfo.setFilter(JSON.toJSONString(((BaseTable) inputTable).getFilter()));
                tableInfo.setBase(true);
                tableInfo.setStorageInfo(InputStorageInfo);
            }
            else
            {
                tableInfo.setFilter(JSON.toJSONString(
                        TableScanFilter.empty(inputTable.getSchemaName(), inputTable.getTableName())));
                tableInfo.setBase(false);
                tableInfo.setStorageInfo(IntermediateStorageInfo);
            }
            partitionInput.setTableInfo(tableInfo);
            partitionInput.setProjection(partitionProjection);
            partitionInput.setOutput(new OutputInfo(outputBase + (outputId++) + "/part", InputStorageInfo, true));
            int[] newKeyColumnIds = rewriteColumnIdsForPartitionedJoin(keyColumnIds, partitionProjection);
            partitionInput.setPartitionInfo(new PartitionInfo(newKeyColumnIds, numPartition));
            partitionInputsBuilder.add(partitionInput);
        }

        return partitionInputsBuilder.build();
    }


    /**
     * Get the join operator of a single left-deep pipeline of joins. All the nodes (joins) in this pipeline
     * have a base table on its right child. Thus, if one node is a join, then it must be the left child of
     * it parent.
     *
     * @param joinedTable the root of the pipeline of joins
     * @param parent the parent, if present, of this pipeline
     * @return the root join operator for this pipeline
     * @throws IOException
     * @throws MetadataException
     */
    private JoinOperator getJoinOperator(JoinedTable joinedTable, Optional<JoinedTable> parent)
        throws IOException, MetadataException
    {
        requireNonNull(joinedTable, "joinedTable is null");
        Join join = requireNonNull(joinedTable.getJoin(), "joinTable.join is null");
        requireNonNull(join.getLeftTable(), "join.leftTable is null");
        requireNonNull(join.getRightTable(), "join.rightTable is null");
        checkArgument(join.getLeftTable().getTableType() == Table.TableType.BASE,"join.leftTable is not base or joined table");
        checkArgument(join.getRightTable().getTableType() == Table.TableType.BASE,"join.rightTable is not base or joined table");

        Table leftTable = join.getLeftTable();
        checkArgument(join.getRightTable().getTableType() == Table.TableType.BASE,
                "join.rightTable is not base table");
        BaseTable rightTable = (BaseTable) join.getRightTable();
        int[] leftKeyColumnIds = requireNonNull(join.getLeftKeyColumnIds(),
                "join.leftKeyColumnIds is null");
        int[] rightKeyColumnIds = requireNonNull(join.getRightKeyColumnIds(),
                "join.rightKeyColumnIds is null");
        JoinType joinType = requireNonNull(join.getJoinType(), "join.joinType is null");
        JoinAlgorithm joinAlgo = requireNonNull(join.getJoinAlgo(), "join.joinAlgo is null");
        if (joinType == JoinType.EQUI_LEFT || joinType == JoinType.EQUI_FULL)
        {
            checkArgument(joinAlgo != JoinAlgorithm.BROADCAST,
                    "broadcast join can not be used for LEFT_OUTER or FULL_OUTER join.");
        }

        List<InputSplit> leftInputSplits = null;
        List<String> leftPartitionedFiles = null;
        // get the rightInputSplits from metadata.
        List<InputSplit> rightInputSplits = getInputSplits(rightTable);
        JoinOperator childOperator = null;

        if (leftTable.getTableType() == Table.TableType.BASE)
        {
            // get the leftInputSplits from metadata.
            leftInputSplits = getInputSplits((BaseTable) leftTable);
            // check if there is a chain join.
            if (joinAlgo == JoinAlgorithm.BROADCAST && parent.isPresent() &&
                    parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.BROADCAST &&
                    parent.get().getJoin().getJoinEndian() == JoinEndian.SMALL_LEFT)
            {
                /*
                * Chain join is found, and this is the first broadcast join in the chain.
                * In this case, we build an incomplete chain join with only two left tables and one ChainJoinInfo.
                */
                BroadcastTableInfo leftTableInfo = getBroadcastTableInfo(
                        leftTable, leftInputSplits, join.getLeftKeyColumnIds());
                BroadcastTableInfo rightTableInfo = getBroadcastTableInfo(
                        rightTable, rightInputSplits, join.getRightKeyColumnIds());
                ChainJoinInfo chainJoinInfo;
                List<BroadcastTableInfo> chainTableInfos = new ArrayList<>();
                // deal with join endian, ensure that small table is on the left.
                if (join.getJoinEndian() == JoinEndian.SMALL_LEFT)
                {
                    chainJoinInfo = new ChainJoinInfo(
                            joinType, join.getLeftColumnAlias(), join.getRightColumnAlias(),
                            parent.get().getJoin().getLeftKeyColumnIds(), join.getLeftProjection(),
                            join.getRightProjection(), false, null);
                    chainTableInfos.add(leftTableInfo);
                    chainTableInfos.add(rightTableInfo);
                }
                else
                {
                    chainJoinInfo = new ChainJoinInfo(
                            joinType.flip(), join.getRightColumnAlias(), join.getLeftColumnAlias(),
                            parent.get().getJoin().getLeftKeyColumnIds(), join.getRightProjection(),
                            join.getLeftProjection(), false, null);
                    chainTableInfos.add(rightTableInfo);
                    chainTableInfos.add(leftTableInfo);
                }

                BroadcastChainJoinInput broadcastChainJoinInput = new BroadcastChainJoinInput();
                broadcastChainJoinInput.setTransId(transId);
                broadcastChainJoinInput.setChainTables(chainTableInfos);
                List<ChainJoinInfo> chainJoinInfos = new ArrayList<>();
                chainJoinInfos.add(chainJoinInfo);
                broadcastChainJoinInput.setChainJoinInfos(chainJoinInfos);

                return new SingleStageJoinOperator(joinedTable.getTableName(), false,
                        broadcastChainJoinInput, JoinAlgorithm.BROADCAST_CHAIN);
            }
        }
        else
        {
            childOperator = getJoinOperator((JoinedTable) leftTable, Optional.of(joinedTable));
            requireNonNull(childOperator, "failed to get child operator");
            // check if there is an incomplete chain join.
            if (childOperator.getJoinAlgo() == JoinAlgorithm.BROADCAST_CHAIN &&
                    !childOperator.isComplete() && // Issue #482: ensure the child operator is complete
                    joinAlgo == JoinAlgorithm.BROADCAST && join.getJoinEndian() == JoinEndian.SMALL_LEFT)
            {
                // the current join is still a broadcast join and the left child is an incomplete chain join.
                if (parent.isPresent() && parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.BROADCAST &&
                        parent.get().getJoin().getJoinEndian() == JoinEndian.SMALL_LEFT)
                {
                    /*
                    * The parent is still a small-left broadcast join, continue chain join construction by
                    * adding the right table of the current join into the left tables of the chain join
                    */
                    BroadcastTableInfo rightTableInfo = getBroadcastTableInfo(
                            rightTable, rightInputSplits, join.getRightKeyColumnIds());
                    ChainJoinInfo chainJoinInfo = new ChainJoinInfo(
                            joinType, join.getLeftColumnAlias(), join.getRightColumnAlias(),
                            parent.get().getJoin().getLeftKeyColumnIds(), join.getLeftProjection(),
                            join.getRightProjection(), false, null);
                    checkArgument(childOperator.getJoinInputs().size() == 1,
                            "there should be exact one incomplete chain join input in the child operator");
                    BroadcastChainJoinInput broadcastChainJoinInput =
                            (BroadcastChainJoinInput) childOperator.getJoinInputs().get(0);
                    broadcastChainJoinInput.getChainTables().add(rightTableInfo);
                    broadcastChainJoinInput.getChainJoinInfos().add(chainJoinInfo);
                    // no need to create a new operator.
                    return childOperator;
                }
                else
                {
                    // The parent is not present or is not a small-left broadcast join, complete chain join construction.
                    boolean postPartition = false;
                    PartitionInfo postPartitionInfo = null;
                    if (parent.isPresent() && parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.PARTITIONED)
                    {
                        // Note: we must use the parent to calculate the number of partitions for post partitioning.
                        postPartition = true;
                        int numPartition = PlanOptimizer.Instance().getJoinNumPartition(
                                this.transId,
                                parent.get().getJoin().getLeftTable(),
                                parent.get().getJoin().getRightTable(),
                                parent.get().getJoin().getJoinEndian());

                        /*
                        * Issue #484:
                        * After we supported multi-pipeline join, this method might be called from getMultiPipelineJoinOperator().
                        * Therefore, the current join might be either the left child or the right child of the parent, and we must
                        * decide whether to use the left key column ids or the right key column ids of the parent as the post
                        * partitioning key column ids.
                        * */
                        if (joinedTable == parent.get().getJoin().getLeftTable())
                        {
                            postPartitionInfo = new PartitionInfo(parent.get().getJoin().getLeftKeyColumnIds(), numPartition);
                        }
                        else
                        {
                            postPartitionInfo = new PartitionInfo(parent.get().getJoin().getRightKeyColumnIds(), numPartition);
                        }

                        /*
                        * For broadcast and broadcast chain join, if every worker in its parent has to
                        * read the outputs of this join, we try to adjust the input splits of its large table.
                        *
                        * If the parent is a large-left broadcast join (small-left broadcast join does not go into
                        * this branch). The outputs of this join will be split into multiple workers and there is
                        * no need to adjust the input splits for this join.
                        */
                        rightInputSplits = adjustInputSplitsForBroadcastJoin(leftTable, rightTable, rightInputSplits);
                    }
                    JoinInfo joinInfo = new JoinInfo(joinType, join.getLeftColumnAlias(), join.getRightColumnAlias(),
                            join.getLeftProjection(), join.getRightProjection(), postPartition, postPartitionInfo);

                    checkArgument(childOperator.getJoinInputs().size() == 1,
                            "there should be exact one incomplete chain join input in the child operator");
                    BroadcastChainJoinInput broadcastChainJoinInput =
                            (BroadcastChainJoinInput) childOperator.getJoinInputs().get(0);

                    ImmutableList.Builder<JoinInput> joinInputs = ImmutableList.builder();
                    int outputId = 0;
                    for (int i = 0; i < rightInputSplits.size();)
                    {
                        ImmutableList.Builder<InputSplit> inputsBuilder = ImmutableList
                                .builderWithExpectedSize(IntraWorkerParallelism);
                        ImmutableList<String> outputs = ImmutableList.of((outputId++) + "/join");
                        for (int j = 0; j < IntraWorkerParallelism && i < rightInputSplits.size(); ++j, ++i)
                        {
                            inputsBuilder.add(rightInputSplits.get(i));
                        }
                        BroadcastTableInfo rightTableInfo = getBroadcastTableInfo(
                                rightTable, inputsBuilder.build(), join.getRightKeyColumnIds());

                        String path = IntermediateFolder + transId + "/" + joinedTable.getSchemaName() + "/" +
                                joinedTable.getTableName() + "/";
                        MultiOutputInfo output = new MultiOutputInfo(path, IntermediateStorageInfo, true, outputs);

                        BroadcastChainJoinInput complete = broadcastChainJoinInput.toBuilder()
                                .setLargeTable(rightTableInfo)
                                .setJoinInfo(joinInfo)
                                .setOutput(output).build();

                        joinInputs.add(complete);
                    }

                    return new SingleStageJoinOperator(joinedTable.getTableName(), true,
                            joinInputs.build(), JoinAlgorithm.BROADCAST_CHAIN);
                }
            }
            // get the leftInputSplits or leftPartitionedFiles from childJoinInputs.
            List<JoinInput> childJoinInputs = childOperator.getJoinInputs();
            if (joinAlgo == JoinAlgorithm.BROADCAST)
            {
                leftInputSplits = getBroadcastInputSplits(childJoinInputs);
            }
            else if (joinAlgo == JoinAlgorithm.PARTITIONED)
            {
                leftPartitionedFiles = getPartitionedFiles(childJoinInputs);
            }
            else
            {
                throw new UnsupportedOperationException("unsupported join algorithm '" + joinAlgo +
                        "' in the joined table constructed by users");
            }
        }

        // generate join inputs for normal broadcast join or partitioned join.
        if (joinAlgo == JoinAlgorithm.BROADCAST)
        {
            requireNonNull(leftInputSplits, "leftInputSplits is null");

            boolean postPartition = false;
            PartitionInfo postPartitionInfo = null;
            if (parent.isPresent() && parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.PARTITIONED)
            {
                postPartition = true;
                // Note: we must use the parent to calculate the number of partitions for post partitioning.
                int numPartition = PlanOptimizer.Instance().getJoinNumPartition(
                        this.transId,
                        parent.get().getJoin().getLeftTable(),
                        parent.get().getJoin().getRightTable(),
                        parent.get().getJoin().getJoinEndian());

                // Check if the current table is the left child or the right child of parent.
                if (joinedTable == parent.get().getJoin().getLeftTable())
                {
                    postPartitionInfo = new PartitionInfo(
                            parent.get().getJoin().getLeftKeyColumnIds(), numPartition);
                }
                else
                {
                    postPartitionInfo = new PartitionInfo(
                            parent.get().getJoin().getRightKeyColumnIds(), numPartition);
                }
            }

            int internalParallelism = IntraWorkerParallelism;
            if (leftTable.getTableType() == Table.TableType.BASE && ((BaseTable) leftTable).getFilter().isEmpty() &&
                    rightTable.getFilter().isEmpty())// && (leftInputSplits.size() <= 128 && rightInputSplits.size() <= 128))
            {
                /*
                * This is used to reduce the latency of join result writing, by increasing the
                * number of function instances (external parallelism).
                * TODO: estimate the cost and tune the parallelism of join by histogram.
                */
                internalParallelism = 2;
            }

            ImmutableList.Builder<JoinInput> joinInputs = ImmutableList.builder();
            if (join.getJoinEndian() == JoinEndian.SMALL_LEFT)
            {
                BroadcastTableInfo leftTableInfo = getBroadcastTableInfo(
                        leftTable, leftInputSplits, join.getLeftKeyColumnIds());

                JoinInfo joinInfo = new JoinInfo(joinType, join.getLeftColumnAlias(), join.getRightColumnAlias(),
                        join.getLeftProjection(), join.getRightProjection(), postPartition, postPartitionInfo);

                if (parent.isPresent() && (parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.PARTITIONED ||
                        (parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.BROADCAST &&
                                parent.get().getJoin().getJoinEndian() == JoinEndian.SMALL_LEFT)))
                {
                    /*
                    * For broadcast and broadcast chain join, if every worker in its parent has to
                    * read the outputs of this join, we try to adjust the input splits of its large table.
                    *
                    * This join is the left child of its parent, therefore, if the parent is a partitioned
                    * join or small-left broadcast join, the outputs of this join will be read by every
                    * worker of the parent.
                    */
                    rightInputSplits = adjustInputSplitsForBroadcastJoin(leftTable, rightTable, rightInputSplits);
                }

                int outputId = 0;
                for (int i = 0; i < rightInputSplits.size();)
                {
                    ImmutableList.Builder<InputSplit> inputsBuilder = ImmutableList
                            .builderWithExpectedSize(internalParallelism);
                    ImmutableList<String> outputs = ImmutableList.of((outputId++) + "/join");
                    for (int j = 0; j < internalParallelism && i < rightInputSplits.size(); ++j, ++i)
                    {
                        inputsBuilder.add(rightInputSplits.get(i));
                    }
                    BroadcastTableInfo rightTableInfo = getBroadcastTableInfo(
                            rightTable, inputsBuilder.build(), join.getRightKeyColumnIds());

                    String path = IntermediateFolder + transId + "/" + joinedTable.getSchemaName() + "/" +
                            joinedTable.getTableName() + "/";
                    MultiOutputInfo output = new MultiOutputInfo(path, IntermediateStorageInfo, true, outputs);

                    BroadcastJoinInput joinInput = new BroadcastJoinInput(
                            transId, leftTableInfo, rightTableInfo, joinInfo,
                            false, null, output);

                    joinInputs.add(joinInput);
                }
            }
            else
            {
                BroadcastTableInfo rightTableInfo = getBroadcastTableInfo(
                        rightTable, rightInputSplits, join.getRightKeyColumnIds());

                JoinInfo joinInfo = new JoinInfo(joinType.flip(), join.getRightColumnAlias(),
                        join.getLeftColumnAlias(), join.getRightProjection(), join.getLeftProjection(),
                        postPartition, postPartitionInfo);

                if (parent.isPresent() && (parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.PARTITIONED ||
                        (parent.get().getJoin().getJoinAlgo() == JoinAlgorithm.BROADCAST &&
                                parent.get().getJoin().getJoinEndian() == JoinEndian.SMALL_LEFT)))
                {
                    /*
                    * For broadcast and broadcast chain join, if every worker in its parent has to
                    * read the outputs of this join, we try to adjust the input splits of its large table.
                    *
                    * This join is the left child of its parent, therefore, if the parent is a partitioned
                    * join or small-left broadcast join, the outputs of this join will be read by every
                    * worker of the parent.
                    */
                    leftInputSplits = adjustInputSplitsForBroadcastJoin(rightTable, leftTable, leftInputSplits);
                }

                int outputId = 0;
                for (int i = 0; i < leftInputSplits.size();)
                {
                    ImmutableList.Builder<InputSplit> inputsBuilder = ImmutableList
                            .builderWithExpectedSize(internalParallelism);
                    ImmutableList<String> outputs = ImmutableList.of((outputId++) + "/join");
                    for (int j = 0; j < internalParallelism && i < leftInputSplits.size(); ++j, ++i)
                    {
                        inputsBuilder.add(leftInputSplits.get(i));
                    }
                    BroadcastTableInfo leftTableInfo = getBroadcastTableInfo(
                            leftTable, inputsBuilder.build(), join.getLeftKeyColumnIds());

                    String path = IntermediateFolder + transId + "/" + joinedTable.getSchemaName() + "/" +
                            joinedTable.getTableName() + "/";
                    MultiOutputInfo output = new MultiOutputInfo(path, IntermediateStorageInfo, true, outputs);

                    BroadcastJoinInput joinInput = new BroadcastJoinInput(
                            transId, rightTableInfo, leftTableInfo, joinInfo,
                            false, null, output);

                    joinInputs.add(joinInput);
                }
            }
            SingleStageJoinOperator joinOperator =
                    new SingleStageJoinOperator(joinedTable.getTableName(), true, joinInputs.build(), joinAlgo);
            if (join.getJoinEndian() == JoinEndian.SMALL_LEFT)
            {
                joinOperator.setSmallChild(childOperator);
            }
            else
            {
                joinOperator.setLargeChild(childOperator);
            }
            return joinOperator;
        }
        else if (joinAlgo == JoinAlgorithm.PARTITIONED)
        {
            // process partitioned join.
            PartitionedJoinOperator joinOperator;
            int numPartition = PlanOptimizer.Instance().getJoinNumPartition(
                    this.transId, leftTable, rightTable, join.getJoinEndian());
            if (childOperator != null)
            {
                // left side is post partitioned, thus we only partition the right table.
                boolean leftIsBase = leftTable.getTableType() == Table.TableType.BASE;
                PartitionedTableInfo leftTableInfo = new PartitionedTableInfo(
                        leftTable.getTableName(), leftIsBase, leftTable.getColumnNames(),
                        leftIsBase ? InputStorageInfo : IntermediateStorageInfo,
                        leftPartitionedFiles, IntraWorkerParallelism, leftKeyColumnIds);

                boolean[] rightPartitionProjection = getPartitionProjection(rightTable, join.getRightProjection());

                List<PartitionInput> rightPartitionInputs = getPartitionInputs(
                        rightTable, rightInputSplits, rightKeyColumnIds, rightPartitionProjection, numPartition,
                        IntermediateFolder + transId + "/" + joinedTable.getSchemaName() + "/" +
                                joinedTable.getTableName() + "/" + rightTable.getTableName() + "/");

                PartitionedTableInfo rightTableInfo = getPartitionedTableInfo(
                        rightTable, rightKeyColumnIds, rightPartitionInputs, rightPartitionProjection);

                List<JoinInput> joinInputs = getPartitionedJoinInputs(
                        joinedTable, parent, numPartition, leftTableInfo, rightTableInfo,
                        null, rightPartitionProjection);

                if (join.getJoinEndian() == JoinEndian.SMALL_LEFT)
                {
                    joinOperator = new PartitionedJoinOperator(joinedTable.getTableName(),
                            null, rightPartitionInputs, joinInputs, joinAlgo);
                    joinOperator.setSmallChild(childOperator);
                }
                else
                {
                    joinOperator = new PartitionedJoinOperator(joinedTable.getTableName(),
                            rightPartitionInputs, null, joinInputs, joinAlgo);
                    joinOperator.setLargeChild(childOperator);
                }
            }
            else
            {
                // partition both tables. in this case, the operator's child must be null.
                boolean[] leftPartitionProjection = getPartitionProjection(leftTable, join.getLeftProjection());
                List<PartitionInput> leftPartitionInputs = getPartitionInputs(
                        leftTable, leftInputSplits, leftKeyColumnIds, leftPartitionProjection, numPartition,
                        IntermediateFolder + transId + "/" + joinedTable.getSchemaName() + "/" +
                                joinedTable.getTableName() + "/" + leftTable.getTableName() + "/");
                PartitionedTableInfo leftTableInfo = getPartitionedTableInfo(
                        leftTable, leftKeyColumnIds, leftPartitionInputs, leftPartitionProjection);

                boolean[] rightPartitionProjection = getPartitionProjection(rightTable, join.getRightProjection());
                List<PartitionInput> rightPartitionInputs = getPartitionInputs(
                        rightTable, rightInputSplits, rightKeyColumnIds, rightPartitionProjection, numPartition,
                        IntermediateFolder + transId + "/" + joinedTable.getSchemaName() + "/" +
                                joinedTable.getTableName() + "/" + rightTable.getTableName() + "/");
                PartitionedTableInfo rightTableInfo = getPartitionedTableInfo(
                        rightTable, rightKeyColumnIds, rightPartitionInputs, rightPartitionProjection);

                List<JoinInput> joinInputs = getPartitionedJoinInputs(
                        joinedTable, parent, numPartition, leftTableInfo, rightTableInfo,
                        leftPartitionProjection, rightPartitionProjection);

                if (join.getJoinEndian() == JoinEndian.SMALL_LEFT)
                {
                    joinOperator = new PartitionedJoinOperator(joinedTable.getTableName(),
                            leftPartitionInputs, rightPartitionInputs, joinInputs, joinAlgo);
                }
                else
                {
                    joinOperator = new PartitionedJoinOperator(joinedTable.getTableName(),
                            rightPartitionInputs, leftPartitionInputs, joinInputs, joinAlgo);
                }
            }
            return joinOperator;
        }
        else
        {
            throw new UnsupportedOperationException("unsupported join algorithm '" + joinAlgo +
                    "' in the joined table constructed by users");
        }
    }

    private List<InputSplit> getInputSplits(BaseTable table) throws MetadataException, IOException
    {
        requireNonNull(table, "table is null");
        checkArgument(table.getTableType() == Table.TableType.BASE, "this is not a base table");
        ImmutableList.Builder<InputSplit> splitsBuilder = ImmutableList.builder();
        int splitSize = 0;
        Storage.Scheme tableStorageScheme =
                metadataService.getTable(table.getSchemaName(), table.getTableName()).getStorageScheme();
        checkArgument(tableStorageScheme.equals(this.storage.getScheme()), String.format(
                "the storage scheme of table '%s.%s' is not consistent with the input storage scheme for Pixels Turbo",
                table.getSchemaName(), table.getTableName()));
        List<Layout> layouts = metadataService.getLayouts(table.getSchemaName(), table.getTableName());
        for (Layout layout : layouts)
        {
            long version = layout.getVersion();
            SchemaTableName schemaTableName = new SchemaTableName(table.getSchemaName(), table.getTableName());
            Ordered ordered = layout.getOrdered();
            ColumnSet columnSet = new ColumnSet();
            for (String column : table.getColumnNames())
            {
                columnSet.addColumn(column);
            }

            // get split size
            Splits splits = layout.getSplits();
            if (this.fixedSplitSize > 0)
            {
                splitSize = this.fixedSplitSize;
            } else
            {
                // log.info("columns to be accessed: " + columnSet.toString());
                SplitsIndex splitsIndex = IndexFactory.Instance().getSplitsIndex(schemaTableName);
                if (splitsIndex == null)
                {
                    logger.debug("splits index not exist in factory, building index...");
                    splitsIndex = buildSplitsIndex(version, ordered, splits, schemaTableName);
                } else
                {
                    long indexVersion = splitsIndex.getVersion();
                    if (indexVersion < version)
                    {
                        logger.debug("splits index version is not up-to-date, updating index...");
                        splitsIndex = buildSplitsIndex(version, ordered, splits, schemaTableName);
                    }
                }
                SplitPattern bestSplitPattern = splitsIndex.search(columnSet);
                splitSize = bestSplitPattern.getSplitSize();
                logger.debug("split size for table '" + table.getTableName() + "': " + splitSize + " from splits index");
                double selectivity = PlanOptimizer.Instance().getTableSelectivity(this.transId, table);
                if (selectivity >= 0)
                {
                    // Increasing split size according to the selectivity.
                    if (selectivity < 0.25)
                    {
                        splitSize *= 4;
                    } else if (selectivity < 0.5)
                    {
                        splitSize *= 2;
                    }
                    if (splitSize > splitsIndex.getMaxSplitSize())
                    {
                        splitSize = splitsIndex.getMaxSplitSize();
                    }
                }
                logger.debug("split size for table '" + table.getTableName() + "': " + splitSize + " after adjustment");
            }
            logger.debug("using split size: " + splitSize);
            int rowGroupNum = splits.getNumRowGroupInFile();

            // get compact path
            String[] compactPaths;
            if (projectionReadEnabled)
            {
                ProjectionsIndex projectionsIndex = IndexFactory.Instance().getProjectionsIndex(schemaTableName);
                Projections projections = layout.getProjections();
                if (projectionsIndex == null)
                {
                    logger.debug("projections index not exist in factory, building index...");
                    projectionsIndex = buildProjectionsIndex(ordered, projections, schemaTableName);
                }
                else
                {
                    int indexVersion = projectionsIndex.getVersion();
                    if (indexVersion < version)
                    {
                        logger.debug("projections index is not up-to-date, updating index...");
                        projectionsIndex = buildProjectionsIndex(ordered, projections, schemaTableName);
                    }
                }
                ProjectionPattern projectionPattern = projectionsIndex.search(columnSet);
                if (projectionPattern != null)
                {
                    logger.debug("suitable projection pattern is found, path='" + projectionPattern.getPaths() + '\'');
                    compactPaths = projectionPattern.getPaths();
                }
                else
                {
                    compactPaths = layout.getCompactPathUris();
                }
            }
            else
            {
                compactPaths = layout.getCompactPathUris();
            }
            logger.debug("using compact path: " + Joiner.on(";").join(compactPaths));

            // get the inputs from storage
            try
            {
                // 1. add splits in orderedPath
                if (orderedPathEnabled)
                {
                    List<String> orderedPaths = storage.listPaths(layout.getOrderedPathUris());

                    for (int i = 0; i < orderedPaths.size();)
                    {
                        ImmutableList.Builder<InputInfo> inputsBuilder =
                                ImmutableList.builderWithExpectedSize(splitSize);
                        for (int j = 0; j < splitSize && i < orderedPaths.size(); ++j, ++i)
                        {
                            InputInfo input = new InputInfo(orderedPaths.get(i), 0, 1);
                            inputsBuilder.add(input);
                        }
                        splitsBuilder.add(new InputSplit(inputsBuilder.build()));
                    }
                }
                // 2. add splits in compactPath
                if (compactPathEnabled)
                {
                    List<String> compactFilePaths = storage.listPaths(compactPaths);

                    int curFileRGIdx;
                    for (String path : compactFilePaths)
                    {
                        curFileRGIdx = 0;
                        while (curFileRGIdx < rowGroupNum)
                        {
                            InputInfo input = new InputInfo(path, curFileRGIdx, splitSize);
                            splitsBuilder.add(new InputSplit(ImmutableList.of(input)));
                            curFileRGIdx += splitSize;
                        }
                    }
                }
            }
            catch (IOException e)
            {
                throw new IOException("failed to get input information from storage", e);
            }
        }

        return splitsBuilder.build();
    }

    private SplitsIndex buildSplitsIndex(long version, Ordered ordered, Splits splits, SchemaTableName schemaTableName)
                throws MetadataException
        {
            List<String> columnOrder = ordered.getColumnOrder();
            SplitsIndex index;
            String indexTypeName = ConfigFactory.Instance().getProperty("splits.index.type");
            SplitsIndex.IndexType indexType = SplitsIndex.IndexType.valueOf(indexTypeName.toUpperCase());
            switch (indexType)
            {
                case INVERTED:
                    index = new InvertedSplitsIndex(version, columnOrder,
                            SplitPattern.buildPatterns(columnOrder, splits), splits.getNumRowGroupInFile());
                    break;
                case COST_BASED:
                    index = new CostBasedSplitsIndex(this.transId, version, this.metadataService, schemaTableName,
                            splits.getNumRowGroupInFile(), splits.getNumRowGroupInFile());
                    break;
                default:
                    throw new UnsupportedOperationException("splits index type '" + indexType + "' is not supported");
            }

            IndexFactory.Instance().cacheSplitsIndex(schemaTableName, index);
            return index;
        }
    
    public Set<Integer> flattenNestedMapOfSets(Map<Integer, Set<Integer>> nestedMap) {
        Set<Integer> flattenedSet = new HashSet<>();

        for (Set<Integer> subset : nestedMap.values()) {
            flattenedSet.addAll(subset);
        }

        return flattenedSet;
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
            if (operands.contains("EnumerableAggregate")){
                partialAggregation = true;
            }
        }
        return partialAggregation;
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

    

}

