package partition.Partition;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class partition_Partitioner extends Partitioner<Text, Text> implements Configurable {

    private Configuration conf = null;

    @Override
    public void setConf(Configuration c) {
        this.conf = c;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public int getPartition(Text key, Text value, int i) {
       String str = key.toString();
       switch(str)
       {
           case "2006":
               return 0;
           case "2007":
               return 1;
           case "2008":
               return 2;
           case "2009":
               return 3;
           case "2010":
               return 4;
           case "2011":
               return 5;
           case "2012":
               return 6;
           case "2013":
               return 7;
           case "2014":
               return 8;
           case "2015":
               return 9;

       }
       return -1;
    }
    
}

