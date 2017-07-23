package minmax.MinMax;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {

	private String month;
	private String town;
	
	public CompositeKeyWritable()
    {
        
    }
    
    public CompositeKeyWritable(String d, String l)
    {
        this.month=d;
        this.town=l;
    }

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public String getTown() {
		return town;
	}

	public void setTown(String town) {
		this.town = town;
	}

	@Override
	public void readFields(DataInput di) throws IOException {
		// TODO Auto-generated method stub
		month = WritableUtils.readString(di);
		town = WritableUtils.readString(di);
		
	}

	@Override
	public void write(DataOutput d) throws IOException {
		 WritableUtils.writeString(d, month);
	      WritableUtils.writeString(d, town);
		
	}

	@Override
	public int compareTo(CompositeKeyWritable o) {
		// TODO Auto-generated method stub
		int result = this.month.compareTo(o.month);
        
        if(result == 0)
                {
                 result = this.town.compareTo(o.town);   
                } 
        return result;
	}
	
	   public String toString()
	    {
	        return (new StringBuilder().append(month).append("\t").append(town).toString());
	    }

}
