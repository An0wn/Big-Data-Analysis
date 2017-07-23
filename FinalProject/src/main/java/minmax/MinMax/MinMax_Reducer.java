package minmax.MinMax;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class MinMax_Reducer extends Reducer<CompositeKeyWritable, Text, CompositeKeyWritable, Text>{
    
   
    public void reduce(CompositeKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
    {
    try{
    int f_count = 0;
    int m_count = 0;
    int v_count = 0;
    int total_count=0;
    float percent_f = 0.0f;
    float percent_m = 0.0f;
    float percent_v = 0.0f;

    String avg = "";

        for (Text val : values)
            {
        	if(val.toString().equalsIgnoreCase("FELONY"))
        	{
        		f_count += 1;        		
        	}
        	if(val.toString().equalsIgnoreCase("MISDEMEANOR"))
        	{
        		m_count += 1;
        	}
        	if(val.toString().equalsIgnoreCase("VIOLATION"))
        	{
        		v_count += 1;
        	}

            }   

        total_count = f_count+m_count+v_count;
        percent_f = (float)((f_count*100)/(total_count));
        percent_m = (float)((m_count*100)/(total_count));
        percent_v = (float)((v_count*100)/(total_count));
        
        avg= "FELONY: "+String.valueOf(percent_f)+"%, "+"MISDEAMENOR: " + String.valueOf(percent_m)+"%, " + "VIOLATION: " + String.valueOf(percent_v)+"% ";
        System.out.println(avg);    
        context.write(key,new Text(avg));
          }
    catch(Exception e)
    {
    	e.printStackTrace();
    }
    }
}
