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

public class tripNumMonth {

	public static class tripNumMonthMapper
      extends Mapper<LongWritable, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context
   	                 ) throws IOException, InterruptedException {

            SimpleDateFormat aDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Calendar cal = Calendar.getInstance();

            //The first line is the field name, ignore it.
			String[] trip_info = value.toString().split(",");
            int duration = Integer.parseInt( trip_info[0] );
            if ( duration != -1 ){
                String start_time = trip_info[1];

                try {
                    Date aDate = aDateFormat.parse(start_time);

                    cal.setTime(aDate);

                    context.write(new Text(Integer.toString( cal.get(Calendar.MONTH) + 1 ) + "-" + Integer.toString( cal.get(Calendar.YEAR) )), one);

                } catch (ParseException pe) {}
            }

		}
	}

	public static class tripNumMonthReducer
	  extends Reducer<Text,IntWritable,Text,IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
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

    Job job = Job.getInstance(conf, "tripNumMonth");

    job.setJarByClass(tripNumMonth.class);

    job.setMapperClass(tripNumMonthMapper.class);

    job.setCombinerClass(tripNumMonthReducer.class);

    job.setReducerClass(tripNumMonthReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
