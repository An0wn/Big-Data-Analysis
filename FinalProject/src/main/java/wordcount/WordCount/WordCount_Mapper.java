package wordcount.WordCount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author krutika
 */
public class WordCount_Mapper extends Mapper<Object, Text, Text, IntWritable>{
 
  private final static IntWritable one = new IntWritable(1);
    
    public void map(Object key,Text value, Context context) throws IOException, InterruptedException{
        
         if(value.toString().contains("LAW_CAT_CD"))
            return;
         
        String[] split = value.toString().split("\t");
        String crime = split[7];
        System.out.println(crime);

            context.write(new Text(crime),one);
           
    }

}
