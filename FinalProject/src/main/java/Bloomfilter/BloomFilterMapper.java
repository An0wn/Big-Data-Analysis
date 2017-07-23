package Bloomfilter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;

public class BloomFilterMapper extends Mapper<Object, Text, Text, NullWritable> {

    Funnel<Crime> cf = new Funnel<Crime>() {

        public void funnel(Crime crime, Sink into) {
            into.putString(crime.offense,Charsets.UTF_8);
        }
    };

    private BloomFilter<Crime> crime_filter = BloomFilter.create(cf, 500, 0.01);

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

       String description;
        
        try {
            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if (files != null && files.length > 0) {

                for (Path file : files) {

                    try {
                        File myFile = new File(file.toUri());
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(myFile.toString()));
                        String line = null;
                        while ((line = bufferedReader.readLine()) != null) {
                            String[] split = line.split("\t");

                            description = String.valueOf(split[0]);
                            Crime cf = new Crime(description);
                            crime_filter.put(cf);
                        }
                    } catch (IOException ex) {
                        System.err.println("Exception while reading  file: " + ex.getMessage());
                    }
                }
            }
        } catch (IOException ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }

    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String values[] = value.toString().split("\t");
        Crime c = new Crime(values[7]);
        if (crime_filter.mightContain(c)) {
            context.write(value, NullWritable.get());
        }

    }

}


