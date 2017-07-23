package InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndex_Mapper extends Mapper<Object, Text, Text, Text> {
    
    private Text addr_cd = new Text();
     private Text crime = new Text();     
   public void map(Object key, Text values, Context context)
   {
        try {
   		 if(values.toString().contains("LAW_CAT_CD"))
	         return;
   		 
            String[] split = values.toString().split("\t");
            crime.set(split[7]);
            addr_cd.set(split[11]);
            
            context.write(addr_cd,crime);
        } catch (IOException | InterruptedException ex) {
            System.out.println("Error in Mapper"+ex.getMessage());
        }
               
   }
}