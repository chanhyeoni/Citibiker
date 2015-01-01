import java.io.IOException;
import java.util.StringTokenizer;

import java.text.DateFormat;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class removeQuo {

	public static class removeQuoMapper
      extends Mapper<LongWritable, Text, Text, Text>{

		public void map(LongWritable key, Text value, Context context
   	                 ) throws IOException, InterruptedException {

            //The first line is the field name, ignore it.
			if (key.get() > 0){
                String[] trip_info = value.toString().split(",");
                String output = "";
                for (int i = 0 ; i < trip_info.length ; i++){
                    trip_info[i] = trip_info[i].substring(1, trip_info[i].length() - 1 );
                    output = output + trip_info[i].trim();
                    if (i < (trip_info.length - 1) ){
                        output = output + ",";
                    }
                }
                context.write(new Text("haha"), new Text(output));
			}

		}
	}

	public static class removeQuoReducer
	  extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

            
			for (Text val : values) {
                context.write(val, null);
			}
            

		}

	}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "removeQuo");

    job.setJarByClass(removeQuo.class);

    job.setMapperClass(removeQuoMapper.class);

    //job.setCombinerClass(removeQuoReducer.class);

    job.setReducerClass(removeQuoReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
