import java.io.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Preprocess_MapReduce {
	// this preproecess_mapreduce code will preprocess the time part of the data
	// such that it only splits the data into month, day, year, and hour and minute
	public static class Preprocess_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text in_value, Context context){
			String line = in_value.toString(); // this will be one line of the text file
			String [] elements = line.split(",");
	        String starttime = elements[1];
	        String[] starttime_elements = starttime.split("[/: ]+");
	        String stoptime = elements[2];
	        String[] stoptime_elements = stoptime.split("[/: ]+");
			try{       
	            
	            String new_key = "";
	            
	            for (int i = 0; i < elements.length; i++){
	            	if(i == 1){continue;}
	            	if(i == 2){continue;}
	            	new_key += elements[i] +  ", ";
	            }
	            
	            for (int i = 0; i < starttime_elements.length; i++){
	            	new_key += starttime_elements[i] + ", ";
	            }
	            
	            for (int i = 0; i < stoptime_elements.length; i++){
	            	new_key += stoptime_elements[i];
	            	if(i!=stoptime_elements.length-1){
	            		new_key += ", ";
	            	}
	        
	            }
				context.write(new Text(new_key), new IntWritable(1));
			}catch(IOException e){
				System.out.println("IOException happened");
			}catch(InterruptedException e){
				System.out.println("Interruption happened");
			}
		}

	}
	
	public static class Preprocess_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context){
			try{
				int sum = 0;
				for (IntWritable i : values){
					sum += i.get();
				}
				context.write(key, new IntWritable(sum));
			}catch(IOException e){
				e.printStackTrace();
			}catch(InterruptedException e){
				e.printStackTrace();
			}
		}

	}
	
	public static class MapReduce {
		// more generalized version of MapReduce code snippet
		// since multiple MapReduce jobs will go through this identical function,
		// it was better to create this function that accepts all kinds of Mapper classes and Reducer classes
		// and run the job.
		// this reduces the redundancy of writing the same code in every single MapReduce part
		
		public static void submitJob(String jobname, String param_1, String param_2,
				Class MapperClass, Class ReducerClass) throws Exception {
			Job job = new Job();
			job.setJobName(jobname);
			job.setJarByClass(MapReduce.class);
			FileInputFormat.addInputPath(job, new Path(param_1));
			FileOutputFormat.setOutputPath(job, new Path(param_2));
			job.setMapperClass(MapperClass);
			job.setReducerClass(ReducerClass);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}

	
	public static void callMapReduce(String raw_filename, String result, String jobname, 
			String output_file, Class MapperClass, Class ReducerClass){
		// gets the Mapreduce functions called checks if the output (result) is already existent
		// if it is, don't call the function, if it is not, call the function
		try{
			File aFile = new File(result);
			String output_path =  output_file;
			if(!aFile.exists()){
				MapReduce.submitJob(jobname, raw_filename, output_path, MapperClass, ReducerClass);}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception{
		// get the MapReduce Job for estimating the frequency table
		String filename = args[0];
		String result = args[1];
		String jobname = args[2];
		String output_path = args[3];
		Class Mapper = Preprocess_Mapper.class;
		Class Reducer = Preprocess_Reducer.class;
		callMapReduce(filename, result, jobname, output_path, Mapper, Reducer);	
	}
}
