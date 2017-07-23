package minmax.MinMax;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {
    
    protected GroupComparator()
    {
        super(CompositeKeyWritable.class,true);
    }
    
    public int compare(WritableComparable w1, WritableComparable w2)
    {
        CompositeKeyWritable cw1 = (CompositeKeyWritable) w1;
        CompositeKeyWritable cw2 = (CompositeKeyWritable) w2;
        
        return cw1.compareTo(cw2);
    }
}
