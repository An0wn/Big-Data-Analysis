package grep.Grep;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Grep_Mapper extends Mapper<Object, Text, NullWritable, Text>{

	private String mapregex = null;
	
	public void setup(Context context)
	{
		mapregex = context.getConfiguration().get("MapRegex");
	}
	
	public void map(Object key, Text value, Context context)
	{
		if(value.toString().matches(mapregex)){
			try {
				context.write(NullWritable.get(), value);
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
