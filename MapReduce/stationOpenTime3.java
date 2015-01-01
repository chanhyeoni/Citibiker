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

public class stationOpenTime3 {

	public static class stationOpenTime3Mapper
      extends Mapper<LongWritable, Text, Text, Text>{

		public void map(LongWritable key, Text value, Context context
   	                 ) throws IOException, InterruptedException {
            //The first line is the field name, ignore it.
			if (key.get() > 0){
				String[] trip_info = value.toString().split(",");

                String start_ID =  trip_info[3].substring(1, trip_info[3].length() - 1 );
                String start_time = trip_info[1].substring(1, trip_info[1].length() - 1 );
                String start_name = trip_info[4].substring(1, trip_info[4].length() - 1 );
                String start_lat = trip_info[5].substring(1, trip_info[5].length() - 1 );
                String start_long = trip_info[6].substring(1, trip_info[6].length() - 1 );

                String stop_ID =  trip_info[7].substring(1, trip_info[7].length() - 1 );
                String stop_time = trip_info[2].substring(1, trip_info[2].length() - 1 );
                String stop_name = trip_info[8].substring(1, trip_info[8].length() - 1 );
                String stop_lat = trip_info[9].substring(1, trip_info[9].length() - 1 );
                String stop_long = trip_info[10].substring(1, trip_info[10].length() - 1 );

                context.write(new Text( start_ID + "," + start_name + "," + start_lat + "," + start_long ), new Text(start_time));
                context.write(new Text( stop_ID + "," + stop_name + "," + stop_lat + "," + stop_long ), new Text(stop_time));
			}

		}
	}

	public static class stationOpenTime3Reducer
	  extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

            String earlyDateStr = "2013-01-01 00:00:00";
            SimpleDateFormat aDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date min_date = new Date();
            Date max_date = new Date();
            try {
                max_date = aDateFormat.parse(earlyDateStr);
            } catch (ParseException pe) {}
            
            int counter = 0;

			for (Text val : values) {

                try {
                    Date aDate = aDateFormat.parse(val.toString());

                    if (aDate.before(min_date)){
                        min_date = aDate;
                    }
                    if (aDate.after(max_date)){
                        max_date = aDate;
                    }

                    counter++;

                } catch (ParseException pe) {
                    context.write(key, new Text("parse exception: " + val.toString()));
                }
                
			}

            context.write(key, new Text( ",\t" + min_date.toString() + ",\t" + max_date.toString() + ",\t" + Integer.toString(counter) ) );

		}

	}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Station Open Time");

    job.setJarByClass(stationOpenTime3.class);

    job.setMapperClass(stationOpenTime3Mapper.class);

    //job.setCombinerClass(stationOpenTimeReducer.class);

    job.setReducerClass(stationOpenTime3Reducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
