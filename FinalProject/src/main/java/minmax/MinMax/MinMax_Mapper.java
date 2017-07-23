package minmax.MinMax;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MinMax_Mapper extends Mapper<Object, Text, CompositeKeyWritable, Text>{
    
	Text type = new Text();
    public void map(Object key, Text value, Context context)
    {
    	 try{
    	 
    	if(value.toString().contains("LAW_CAT_CD"))
    	            return;
    	        
    	String split[] = value.toString().split("\t");
    	String date[] = split[6].toString().split("/");
        type.set(split[12]);
        CompositeKeyWritable cw = new CompositeKeyWritable(date[0],split[14]);
            context.write(cw, new Text(type));
        }
        catch(IOException | InterruptedException ex){
        System.out.println("Error Message" + ex.getMessage());
        }
    }
}