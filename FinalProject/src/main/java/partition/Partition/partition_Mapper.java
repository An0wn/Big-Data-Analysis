package partition.Partition;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class partition_Mapper extends Mapper<Object, Text, Text, Text> {
    
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
try{
	
    	if(value.toString().contains("LAW_CAT_CD"))
            return;
        
        String split[] = value.toString().split("\t");
        String date[] = split[5].toString().split("/");
        String year = date[2];
        
        context.write(new Text(year), value);
    	}
	catch(Exception e)
	{
	e.printStackTrace();
	}
    }
}