package topten.Topten;

import java.io.IOException;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopTenMapper2 extends Mapper<Object, Text, Text, Text> {
	 
	private TreeMap<Integer, Text> areamap = new TreeMap<Integer, Text>();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] row = (value.toString()).split("\t");
        Text area = new Text(row[0]);
        int num_crimes  = Integer.parseInt(row[1].trim());
        
        areamap.put(num_crimes, area);
        
        if(areamap.size()>10) {
			areamap.remove(areamap.firstKey());
		}
        
	}

	public void cleanup(Context context)
			throws IOException, InterruptedException {
		for(int key : areamap.descendingMap().keySet()) {
			context.write(new Text(areamap.get(key)), new Text(String.valueOf(key)));
		}
	}
	
	

}
