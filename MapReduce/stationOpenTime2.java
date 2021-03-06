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

public class stationOpenTime2 {

	public static class stationOpenTime2Mapper
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

                context.write(new Text( "(" + start_lat + ", " + start_long + ")" ), new Text(start_time + "," + start_ID + "," + start_name ));
                context.write(new Text( "(" + stop_lat + ", " + stop_long + ")" ), new Text(stop_time + "," + stop_ID + "," + stop_name ));
			}

		}
	}

	public static class stationOpenTime2Reducer
	  extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

            SimpleDateFormat aDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date min_date = new Date();
            String station_info = "";

			for (Text val : values) {
                
                String[] split_val = val.toString().split(",");
                
                if (station_info == ""){
                    station_info = split_val[1] + ",\t" + split_val[2];
                }

                try {
                    Date aDate = aDateFormat.parse(split_val[0]);

                    if (aDate.before(min_date)){
                        min_date = aDate;
                    }

                } catch (ParseException pe) {
                    context.write(key, new Text("parse exception: " + split_val[0]));
                }
                
			}

            context.write(key, new Text(min_date.toString() + ",\t" + station_info));

		}

	}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Station Open Time");

    job.setJarByClass(stationOpenTime2.class);

    job.setMapperClass(stationOpenTime2Mapper.class);

    //job.setCombinerClass(stationOpenTimeReducer.class);

    job.setReducerClass(stationOpenTime2Reducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
