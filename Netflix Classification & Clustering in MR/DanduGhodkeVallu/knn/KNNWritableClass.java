import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.TestMapCollection.KeyWritable;

public class KNNWritableClass implements WritableComparable{
	
	
	private Text movieName;
	private DoubleWritable distance;
	
	public KNNWritableClass() {
		// TODO Auto-generated constructor stub
		this.movieName = new Text();
		this.distance = new DoubleWritable();
	}
	
	public KNNWritableClass(Text code, DoubleWritable distance) {
		// TODO Auto-generated constructor stub
		this.movieName = code;
		this.distance = distance;
	}
	
	public Text getmovieName() {
		return movieName;
	}

	public void setmovieName(Text movieName) {
		this.movieName = movieName;
	}

	public DoubleWritable getdistance() {
		return distance;
	}

	public void setdistance(DoubleWritable distance) {
		this.distance = distance;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		getmovieName().write(out);
		getdistance().write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		getmovieName().readFields(in);
		getdistance().readFields(in);
	}

	
	@Override
	public int hashCode() {
		// TODO Auto-generated method stub
		return getmovieName().hashCode();
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		Double previousdistance, presentdistance;
		previousdistance = Double.parseDouble(this.distance.toString());
		presentdistance = Double.parseDouble(((KNNWritableClass) o).distance.toString());
		int result = this.movieName.compareTo(((KNNWritableClass) o).getmovieName());
		if (result == 0){
			if(presentdistance == presentdistance){
				return 0;
			}
			else{
				if(presentdistance<presentdistance){
					return -1;
				}
				else{
					return 1;
				}
			}
		}else {return result;}
	}
	
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return this.movieName.equals(((KNNWritableClass)obj).getmovieName()) && 
				this.distance.equals(((KNNWritableClass)obj).getdistance());
	}
	
}