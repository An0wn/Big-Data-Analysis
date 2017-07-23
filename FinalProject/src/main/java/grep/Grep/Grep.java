package grep.Grep;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Grep {

	  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	        // TODO code application logic here
	       try{ 
	        Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "MR_Partitioning");
	        job.setJarByClass(Grep.class);
	        job.setMapperClass(Grep_Mapper.class);
	        
	        job.setNumReduceTasks(0);
	        job.setMapOutputKeyClass(NullWritable.class);
	        job.setMapOutputValueClass(Text.class);
	        job.setOutputKeyClass(NullWritable.class);
	        job.setOutputValueClass(Text.class);
	        
	        conf.set("MapRegex", args[1]);
	        FileInputFormat.addInputPath(job,new Path(args[0]));
	        FileOutputFormat.setOutputPath(job,new Path(args[2]));
	        System.exit(job.waitForCompletion(true) ? 0 :1);
	    }
	  catch(Exception e)
	  {
		  
	  }
	  }
}