import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.opencsv.*;


public class KNNSecondarySort{
	public static int kValue = 5;
	
	public static class KnnMapper extends Mapper<Object,Text,KNNWritableClass,Text>{
		//Integer l=0;
		private ArrayList<String> testInstance = new ArrayList<String>();
		private ArrayList<String> trainInstance = new ArrayList<String>();
		private BufferedReader br;
		@Override
		protected void setup(
				Mapper<Object, Text, KNNWritableClass, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.setup(context);
		
			 try{
                Path uri[] = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				BufferedReader br = new BufferedReader(new FileReader(uri[0].toString()));
                String line;
                line=br.readLine();
                CSVParser parser = new CSVParser('\t');
                String testMovies [];
                 
                int i=0;
                
                while (line != null){
                	
                		line=br.readLine();
                    	
                    	if(null != parser.parseLine(line)){
                    		String concateString = null;
                            testMovies = parser.parseLine(line);
	                    	//System.out.println(testMovies[0]);
	                        concateString = testMovies[0]+";"+testMovies[1]+";"+testMovies[2];
	                        testInstance.add(concateString);
	                        i=i+1;
                    	}
                }

			 }catch(Exception e){
				 e.printStackTrace();
			 }
			 
		}
		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, KNNWritableClass, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			
		
			if(value!=null){
				CSVParser parser = new CSVParser('\t');
				String movies[] = new String[10];//movie,year,avg-rating
				movies = parser.parseLine(value.toString());
				//find the length of the movie
				String concateString = null;
				concateString = movies[0]+";"+movies[1]+";"+movies[2];
                
				trainInstance.add(concateString);
				
				
			}			
		}
		@Override
		protected void cleanup(
				Mapper<Object, Text, KNNWritableClass, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.cleanup(context);
		
		
			for(int i = 0; i < testInstance.size();i++){
				String testMatrix = (String)testInstance.get(i);
				String testMatrixRes[] = testMatrix.split(";");
//				System.out.println(testMatrixRes[0] +" : Value of i :"+ i + ": Year" + testMatrixRes[1]);

				String testMovieName = testMatrixRes[0];
				int testMovieLength = testMatrixRes[0].length();
				int testYear = Integer.parseInt(testMatrixRes[1]);
							
				ArrayList<String> result = new ArrayList<String>();
				for(int j=0; j < trainInstance.size();j++){
					String trainMatrix = (String)trainInstance.get(j);
					String trainMatrixRes[] = trainMatrix.split(";");
					String trainMovieName = trainMatrixRes[0];
					int trainMovieLength = trainMatrixRes[0].length();
					int trainYear = Integer.parseInt(trainMatrixRes[1]);
					Float trainRating = Float.parseFloat(trainMatrixRes[2]);
					String rating = null;
					Float distance = (float) Math.sqrt(Math.pow((testMovieLength-trainMovieLength),2) + 
							Math.pow((testYear-trainYear),2));
//					System.out.println(distance+"distance");
					if(trainRating < 2.2)
						rating = "Bad";
					else if(trainRating > 2.2 && trainRating < 3.2)
						rating = "Average";
					else
						rating = "Good";
					String resultSet = distance+";"+trainMovieName+";"+trainYear+";"+rating+";"+trainRating.toString();
//					System.out.println(resultSet);
					result.add(resultSet);
					
				}
				
				Collections.sort(result);
				
				int k = 0;
				
				for(String sortedvalues : result){
					//System.out.println("Printing sorted values"+sortedvalues + " Key for them" + testMovieName);
					String values [] = sortedvalues.split(";");
					
					KNNWritableClass movieDistance = new KNNWritableClass();
					movieDistance.setmovieName(new Text(testMovieName+" : "+testYear));
					movieDistance.setdistance(new DoubleWritable(Double.parseDouble(values[0].toString())));
					 
					Text value = new Text(sortedvalues);
					context.write(movieDistance, value);
					k++;
					if(k == kValue)
						break;
					
				}
			}
		}
	}
	
	public static class KNNSortComparator
	extends WritableComparator{
		
		
		protected KNNSortComparator() {
			super(KNNWritableClass.class, true);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			KNNWritableClass KNNKey1 = (KNNWritableClass) a;
			KNNWritableClass KNNKey2 = (KNNWritableClass) b;
			
			return KNNKey1.compareTo(KNNKey2);
		}
	}
	
	public static class KNNGroupComparator
	extends WritableComparator{
		
		
		protected KNNGroupComparator() {
			super(KNNWritableClass.class, true);
			// TODO Auto-generated constructor stub
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			KNNWritableClass KNNKey1 = (KNNWritableClass) a;
			KNNWritableClass KNNKey2 = (KNNWritableClass) b;
			
			return KNNKey1.getmovieName().compareTo(KNNKey2.getmovieName());
		}
	}
	
	public static class CustomPartitioner extends Partitioner<KNNWritableClass, Text>{
		@Override
		public int getPartition(KNNWritableClass key, Text value, int numPartitions) {
			// TODO Auto-generated method stub
			//check
			return Math.abs(key.hashCode() * 127) % numPartitions;
		}
	}
	
	
	public static class  KNNReducer extends Reducer<KNNWritableClass, Text, Text, Text>{
		
		@Override
		protected void reduce(KNNWritableClass key, java.lang.Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer<KNNWritableClass,Text,Text,Text>.Context context) throws IOException ,InterruptedException {
		
			HashMap<String,Integer> classKey = new HashMap<String,Integer>();
			HashMap<String,Double> distanceKey = new HashMap<String,Double>();
			
			int k = 0;
			while(k < kValue){
				for(Text val : values){
					String records[] = val.toString().split(";");
					
					if(classKey.containsKey(records[3])){
						classKey.put(records[3], classKey.get(records[3])+1);
					}else{
						classKey.put(records[3],1); 
					}if(distanceKey.containsKey(records[3])){
						distanceKey.put(records[3], distanceKey.get(records[3])+ 
								Double.parseDouble(records[0]));
					}else{
						distanceKey.put(records[3],Double.parseDouble(records[0]));
					}
				}
				
				k++;
			}
			int max = 0;
			
			String className = null;
			
			for(Map.Entry<String, Integer> entry : classKey.entrySet()){
				
				if(entry.getValue()>max){
					max = entry.getValue();
					className = entry.getKey();
				}
			}
			
			context.write(key.getmovieName(),new Text(className + " : " + max));
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
		      System.err.println("Usage: flightdata <in> <out>");
		      System.exit(2);
		}
			Job job = new Job(conf, "KNN");
			job.setJarByClass(KNNSecondarySort.class);
			job.setMapperClass(KnnMapper.class);
		    job.setGroupingComparatorClass(KNNGroupComparator.class);
		    job.setReducerClass(KNNReducer.class);	    
			job.setPartitionerClass(CustomPartitioner.class);
		    job.setSortComparatorClass(KNNSortComparator.class);
	    	job.setNumReduceTasks(5);
			job.setOutputKeyClass(KNNWritableClass.class);
			job.setOutputValueClass(Text.class);
			
			Path pth = new Path(args[0]);
			DistributedCache.addCacheFile(pth.toUri(), job.getConfiguration());
			FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
			job.waitForCompletion(true);
		}
	}

	
