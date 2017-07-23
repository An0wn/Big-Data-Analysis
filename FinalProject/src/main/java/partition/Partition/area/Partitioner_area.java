package partition.Partition.area;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Partitioner_area {

	  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	        // TODO code application logic here
	       try{ 
	        Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "MR_Partitioning");
	        job.setJarByClass(Partitioner_area.class);
	        job.setMapperClass(partition_area_Mapper.class);
	        job.setReducerClass(partition_area_Reducer.class);
	        job.setPartitionerClass(partition_area_Partitioner.class);
	        
	        job.setNumReduceTasks(10);
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setOutputKeyClass(NullWritable.class);
	        job.setOutputValueClass(Text.class);
	        
	        FileInputFormat.addInputPath(job,new Path(args[0]));
	        FileOutputFormat.setOutputPath(job,new Path(args[1]));
	        System.exit(job.waitForCompletion(true) ? 0 :1);
	    }
	  catch(Exception e)
	  {
		  
	  }
	  }
}
