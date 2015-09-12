import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import au.com.bytecode.opencsv.CSVParser;

public class HPopulate {
	static long count =1;
	static Configuration config = HBaseConfiguration.create();
	static HBaseAdmin admin;
	static HTableDescriptor desc;
	// Creating the tables
	
	public static class HPopulateMapper extends
	Mapper<Object, Text, NullWritable, NullWritable> {
		HTable table;
		// Setting up the data required
		public void setup(Context context) throws IOException, InterruptedException{
			table = new HTable(config,"DataAll");
			table.setAutoFlush(false);
		}
		
		// Map function
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			CSVParser parser = new CSVParser(',');
			String[] contents = parser.parseLine(value.toString());
			String movieId = contents[0];
			String movieYear = contents[1];
			String movieTitle = contents[2];
			
			// Data should be of same length
			StringBuilder sb = new StringBuilder();
			sb.append("movie").append(movieId);
			byte[] keyData = Bytes.toBytes(sb.toString());
			//System.out.println(sb.toString());
			Put p = new Put(keyData);
			p.add("MovieData".getBytes(), "value".getBytes(), (movieTitle+";"+movieId).getBytes());
			table.put(p);
		}
		
		// Cleanup function
		public void cleanup(Context context) throws IOException{
			table.close();
		}
	}
	
	// The main function
	public static void main(String args[]) throws Exception{
		admin = new HBaseAdmin(config);
		if (args.length != 2) {
			System.err.println("Usage: HPopulate <in> <out>");
			System.exit(2);
		}
		if(!admin.tableExists("DataAll")){
			System.out.println("Creating the table");
			desc = new HTableDescriptor("DataAll");
			desc.addFamily(new HColumnDescriptor("MovieData"));
			admin.createTable(desc);
		}else{
			System.out.println("Table exists");
		}
		Configuration conf = new Configuration();
		Job job = new Job(conf, "HPopulate");
		job.setJarByClass(HPopulate.class);
		job.setMapperClass(HPopulateMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	

}
