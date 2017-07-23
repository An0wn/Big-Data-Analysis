package sorting.Sorting;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable, WritableComparable<CompositeKeyWritable> {

	private String department;
	private String category;
	
	public CompositeKeyWritable()
    {
        
    }
    
    public CompositeKeyWritable(String d, String l)
    {
        this.department=d;
        this.category=l;
    }
   
	public String getDepartment() {
		return department;
	}

	public void setDepartment(String department) {
		this.department = department;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	@Override
	public void readFields(DataInput di) throws IOException {
		// TODO Auto-generated method stub
		department = WritableUtils.readString(di);
		category = WritableUtils.readString(di);
		
	}

	@Override
	public void write(DataOutput d) throws IOException {
		 WritableUtils.writeString(d, department);
	      WritableUtils.writeString(d, category);
		
	}

	@Override
	public int compareTo(CompositeKeyWritable o) {
		// TODO Auto-generated method stub
		int result = this.department.compareTo(o.department);
        
        if(result == 0)
                {
                 result = this.category.compareTo(o.category);   
                } 
        return result;
	}
	
	   public String toString()
	    {
	        return (new StringBuilder().append(department).append("\t").append(category).toString());
	    }

}
