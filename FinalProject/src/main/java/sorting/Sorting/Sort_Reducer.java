package sorting.Sorting;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class Sort_Reducer extends Reducer<CompositeKeyWritable,IntWritable,CompositeKeyWritable,IntWritable>
{    
    IntWritable result = new IntWritable();
    public void reduce(CompositeKeyWritable key, Iterable<IntWritable> values, Context context)
    {
    	int sum = 0;
    	try{
        for(IntWritable val:values)
        {
        	sum += 1;
            
        }
        result.set(sum);
        context.write(key, result);
    }
        catch(IOException | InterruptedException ex){
            System.err.println("Error Message" + ex.getMessage());
        }
    }
}
