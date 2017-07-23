package sampling.Sampling;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Sampling_Mapper extends Mapper<Object, Text, NullWritable, Text>{

	private Random rands = new Random();
	private Double percent;
	
	protected void setup(Context context) throws IOException, InterruptedException{
		String strPercent = context.getConfiguration().get("filterpercent");
		percent = Double.parseDouble(strPercent)/100.0;
	}
	
	public void map(Object key, Text value, Context context){
		
		if(rands.nextDouble() < percent){
			try {
				context.write(NullWritable.get(),value);
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
