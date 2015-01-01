import java.io.IOException;
import java.util.StringTokenizer;

import java.text.DateFormat;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;

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

public class tripDuration {

	public static class tripDurationMapper
      extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

        private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context
   	                 ) throws IOException, InterruptedException {
            
            //The first line is the field name, ignore it.
            String[] trip_info = value.toString().split(",");
            int duration = Integer.parseInt( trip_info[0] );
            if ( duration != -1 ){
                context.write(new IntWritable( duration/60 ), one);
            }
            

		}
	}

	public static class tripDurationReducer
	  extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

		public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
		}

	}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "trip Duration");

    job.setJarByClass(tripDuration.class);

    job.setMapperClass(tripDurationMapper.class);

    job.setCombinerClass(tripDurationReducer.class);

    job.setReducerClass(tripDurationReducer.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
