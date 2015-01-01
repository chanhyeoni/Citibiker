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

public class tripWeekday {

	public static class tripWeekdayMapper
      extends Mapper<LongWritable, Text, Text, Text>{

		public void map(LongWritable key, Text value, Context context
   	                 ) throws IOException, InterruptedException {

            SimpleDateFormat aDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String one = "1";
            String zero = "0"; 

            String[] trip_info = value.toString().split(",");
            int duration = Integer.parseInt( trip_info[0] );

            if ( duration != -1){

                String start_time = trip_info[1];
                String end_time = trip_info[2];
                String start_lat = trip_info[5];
                String start_long = trip_info[6];
                String end_lat = trip_info[9];
                String end_long = trip_info[10];
                String user_type = trip_info[12];

                if ( user_type.equals("Subscriber") ){

                    try {
                        Date aDateStart = aDateFormat.parse(start_time);
                        Date aDateStop = aDateFormat.parse(end_time);

                        context.write(new Text(zero), 
                                   new Text( trip_info[0] + ","
                                 + new SimpleDateFormat("EEE").format( aDateStart ) + ","
                                 + new SimpleDateFormat("MMM").format( aDateStart )  + ","
                                 + new SimpleDateFormat("dd").format( aDateStart ) + ","
                                 + new SimpleDateFormat("yyyy").format( aDateStart ) + ","
                                 + new SimpleDateFormat("HH").format( aDateStart )  + ","
                                 + new SimpleDateFormat("EEE").format( aDateStop ) + ","
                                 + new SimpleDateFormat("MMM").format( aDateStop )  + ","
                                 + new SimpleDateFormat("dd").format( aDateStop ) + ","
                                 + new SimpleDateFormat("yyyy").format( aDateStop ) + ","
                                 + new SimpleDateFormat("HH").format( aDateStop )  + ","
                                 + trip_info[3] + "," + trip_info[4] + "," + trip_info[5] + ","
                                 + trip_info[6] + "," + trip_info[7] + "," + trip_info[8] + ","
                                 + trip_info[9] + "," + trip_info[10] + "," + trip_info[11] + ","
                                 + trip_info[12] + "," + trip_info[13] + "," + trip_info[14]
                                    ));
                    
                    } catch (ParseException pe) {
                    
                    }

                }

            }

		}
	}

	public static class tripWeekdayReducer
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

    Job job = Job.getInstance(conf, "tripWeekday");

    job.setJarByClass(tripWeekday.class);

    job.setMapperClass(tripWeekdayMapper.class);

    //job.setCombinerClass(tripWeekdayReducer.class);

    job.setReducerClass(tripWeekdayReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
