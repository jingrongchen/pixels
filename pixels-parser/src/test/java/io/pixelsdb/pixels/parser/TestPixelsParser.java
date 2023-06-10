/*
 * Copyright 2023 PixelsDB.
 *
 * This file is part of Pixels.
 *
 * Pixels is free software: you can redistribute it and/or modify
 * it under the terms of the Affero GNU General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * Pixels is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Affero GNU General Public License for more details.
 *
 * You should have received a copy of the Affero GNU General Public
 * License along with Pixels.  If not, see
 * <https://www.gnu.org/licenses/>.
 */
package io.pixelsdb.pixels.parser;

import io.pixelsdb.pixels.common.metadata.MetadataService;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.rel.RelVisitor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;

import java.io.*;


import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.plan.RelOptUtil;

import org.apache.calcite.rel.externalize.RelWriterImpl;

import java.util.List;

import java.util.Properties;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class TestPixelsParser
{
    String hostAddr = "ec2-18-218-128-203.us-east-2.compute.amazonaws.com";

    MetadataService instance = null;

    PixelsParser tpchPixelsParser = null;

    @Before
    public void init()
    {
        this.instance = new MetadataService(hostAddr, 18888);
        SqlParser.Config parserConfig = SqlParser.configBuilder()
                .setLex(Lex.MYSQL_ANSI)
                .setParserFactory(SqlParserImpl.FACTORY)
                .build();

        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");

        this.tpchPixelsParser = new PixelsParser(this.instance, "tpch", parserConfig, properties);
    }

    @After
    public void shutdown() throws InterruptedException
    {
        this.instance.shutdown();
    }

    @Test
    public void testPixelsParserTpchExample() throws SqlParseException
    {
        String query = TpchQuery.Q8;
        SqlNode parsedNode = this.tpchPixelsParser.parseQuery(query);
        System.out.println("Parsed SQL Query: \n" + parsedNode);

        SqlNode validatedNode = this.tpchPixelsParser.validate(parsedNode);
        System.out.println("No exception, validation success.");

        RelNode rel = this.tpchPixelsParser.toRelNode(validatedNode);
        final RelJsonWriter writer = new RelJsonWriter();
        rel.explain(writer);
        System.out.println("Logical plan: \n" + writer.asString());
    }

    public static String replaceBlank(String str) {
        String dest = "";
        if (str!=null) {
            Pattern p = Pattern.compile("\t|\r|\n");
            Matcher m = p.matcher(str);
            dest = m.replaceAll(" ");
        }
        return dest;
    }

    @Test
    public void testPixelsParserTestExample() throws SqlParseException{

    try{
        String query = TestQuery.TPCHQ1;
        // String filePath = "/home/ubuntu/opt/lambda-java8/tpchsql/Q1.sql";
        // byte[] bytes = Files.readAllBytes(Paths.get(filePath));
        // String query = new String(bytes, "UTF-8");
        // query = replaceBlank(query);
        // System.out.println(query);

        SqlNode parsedNode = this.tpchPixelsParser.parseQuery(query);
        // System.out.println("Parsed SQL Query: \n" + parsedNode);

        SqlNode validatedNode = this.tpchPixelsParser.validate(parsedNode);
        System.out.println("No exception, validation success.");

        RelNode rel = this.tpchPixelsParser.toRelNode(validatedNode);

        
        // RelVisitor visitor = new RelVisitor() {
        //     @Override
        //     public void visit(RelNode node, int ordinal, RelNode parent) {
        //         // node.getInputs().forEach(input -> {
        //         //     System.out.println("Node: "+ input.getId() + "  "+ input.getClass().getSimpleName());
        //         //     System.out.println(input.getDigest());
        //         // });
                
        //         System.out.println(node.getDigest());
                
        //         super.visit(node, ordinal, parent);
        //     }
        // };
        // visitor.go(rel);
        
        final RelJsonWriter writer = new RelJsonWriter();    
        rel.explain(writer);
        System.out.println(writer.asString());
        System.out.println("Logical plan: \n" + writer.asString());

        try {
            BufferedWriter out = new BufferedWriter(new FileWriter("/home/ubuntu/opt/pixels/pixels-parser/src/test/java/io/pixelsdb/pixels/parser/tpchq1.json"));
            out.write(writer.asString());
            out.close();
            System.out.println("to finle success！");
        } catch (IOException e) {
        }
        
        }catch(Exception e){
            e.printStackTrace();
        }

    }


    @Test
    public void testPixelsParserTest3Example() throws SqlParseException{

        // SqlNode sqlTree = optimizer.parse(sql);
        // SqlNode validatedSqlTree = optimizer.validate(sqlTree);
        // RelNode relTree = optimizer.convert(validatedSqlTree);


        String query = TestQuery.Q3;
        SqlNode parsedNode = this.tpchPixelsParser.parseQuery(query);
        System.out.println("Parsed SQL Query: \n" + parsedNode);

        SqlNode validatedNode = this.tpchPixelsParser.validate(parsedNode);
        System.out.println("No exception, validation success.");

        RelNode rel = this.tpchPixelsParser.toRelNode(validatedNode);
        final RelJsonWriter writer = new RelJsonWriter();
        rel.explain(writer);

        
        
        System.out.println("Logical plan: \n" + writer.asString());

        try {
            BufferedWriter out = new BufferedWriter(new FileWriter("/home/ubuntu/opt/pixels/pixels-parser/src/test/java/io/pixelsdb/pixels/parser/testlogicalPlan2.json"));
            out.write(writer.asString());
            out.close();
            System.out.println("to finle success！");
        } catch (IOException e) {
        }
        

    }

    @Test(expected = SqlParseException.class)
    public void testParserInvalidSyntaxFailure() throws SqlParseException
    {
        String invalidSyntaxQuery = "select * from CUSTOMER AND";
        SqlNode parsedNode = this.tpchPixelsParser.parseQuery(invalidSyntaxQuery);
    }

    @Test(expected = CalciteContextException.class)
    public void testValidatorNonExistentColumnFailure() throws SqlParseException
    {
        String wrongColumnQuery = "select s_name from LINEITEM";
        SqlNode parsedNode = this.tpchPixelsParser.parseQuery(wrongColumnQuery);
        SqlNode validatedNode = this.tpchPixelsParser.validate(parsedNode);
    }

    @Test(expected = CalciteContextException.class)
    public void testValidatorNonExistentTableFailure() throws SqlParseException
    {
        String wrongTableQuery = "select * from VOIDTABLE";
        SqlNode parsedNode = this.tpchPixelsParser.parseQuery(wrongTableQuery);
        SqlNode validatedNode = this.tpchPixelsParser.validate(parsedNode);
    }
}
