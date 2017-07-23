package sorting.Sorting;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Sort_Mapper extends Mapper<Object, Text, CompositeKeyWritable, IntWritable>{
    
	  private final static IntWritable one = new IntWritable(1);
    public void map(Object key, Text value, Context context)
    {
    	 try{
    	 
    		 if(value.toString().contains("LAW_CAT_CD"))
    	         return;
    		 
        String values[] = value.toString().split("\t");
        CompositeKeyWritable cw = new CompositeKeyWritable(values[12],values[11]);
            context.write(cw,one);
        }
        catch(IOException | InterruptedException ex){
        System.out.println("Error Message" + ex.getMessage());
        }
    }
}
