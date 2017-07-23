package sorting.Sorting;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import minmax.MinMax.MinMax_Reducer;

public class Sort {

	public static void main(String[] args) throws InterruptedException {
        try{
      Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Sort");
        job.setJarByClass(Sort.class);
        job.setMapperClass(Sort_Mapper.class);
        job.setCombinerClass(Sort_Reducer.class);
        job.setReducerClass(Sort_Reducer.class);
        job.setMapOutputKeyClass(CompositeKeyWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setGroupingComparatorClass(GroupComparator.class);
        
        job.setOutputKeyClass(CompositeKeyWritable.class);
        job.setOutputValueClass(IntWritable.class);
         
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
        catch(IOException | ClassNotFoundException ex)
                {
                    Logger.getLogger(Sort.class.getName()).log(Level.SEVERE,null,ex);
                }
    
}
}
