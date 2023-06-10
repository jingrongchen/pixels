package io.pixelsdb.pixels.parser;
import java.sql.* ;
import org.junit.Test;
import java.nio.file.Files;
import java.nio.file.Paths;

public class TestPrepareQuery {
    

    @Test
    public void genQueryString(){
        String filePath = "/home/ubuntu/opt/lambda-java8/tpchsql/Q1.sql";
        try{
            byte[] bytes = Files.readAllBytes(Paths.get(filePath));
            String query = new String(bytes, "UTF-8");
            System.out.println(query);
        }catch(Exception e){
            e.printStackTrace();
        }


    }

    
}
