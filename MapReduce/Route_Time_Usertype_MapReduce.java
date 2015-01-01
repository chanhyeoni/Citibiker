import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Route_Time_Usertype_MapReduce {
	
	public static class ManyAttributes_Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text in_value, Context context){
			try{
				String line = in_value.toString(); // this will be one line of the text file
				String[] elements = line.split(",");
				String starttime = elements[1];
				String stoptime = elements[2];
				String start_name = elements[4];
				String stop_name = elements[8];
				String usertype = elements[12];
				String new_key = starttime + ", " + stoptime + ", "
				+ start_name + ", " + stop_name + ", "
				+ usertype + ", ";
				context.write(new Text(new_key), new IntWritable(1));
			}catch(IOException e){
				e.printStackTrace();
			}catch(InterruptedException e){
				e.printStackTrace();
			}
		}
	}
	
	public static class ManyAttributes_Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		// this class gets the intermediate key/value pairs of the word and 1,
		// matches the pairs with the same keys
		// and generate the new key/value pairs whose values are the number of the occurrence
		// of the words
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context){
			try{
				int sum = 0;
				for (IntWritable value : values) {
					 sum+= value.get();
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
			FileInputFormat.addInputPath(job, new Path(param_1)); // must be "./data/2014_08"
			FileOutputFormat.setOutputPath(job, new Path(param_2)); // must be RouteFrequencyCountTable
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
			String output_path = output_file;
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
		Class Mapper = ManyAttributes_Mapper.class;
		Class Reducer = ManyAttributes_Reducer.class;

		callMapReduce(filename, result, jobname, output_path, Mapper, Reducer);
		//SparkConf conf = new SparkConf().setAppName("Spark").setMaster("local[*]");
		//JavaSparkContext sc = new JavaSparkContext(conf);
		//JavaRDD<String> rawFile = sc.textFile(filename);
		//System.out.println("read the file!");
		//JavaRDD<Historical_Data_Record> aData = Spark.get_Historical_Data(sc, filename);	
		
	}

}
