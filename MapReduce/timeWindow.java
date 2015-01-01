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

public class timeWindow {

	public static class timeWindowMapper
      extends Mapper<LongWritable, Text, Text, Text>{

		public void map(LongWritable key, Text value, Context context
   	                 ) throws IOException, InterruptedException {

            SimpleDateFormat aDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String one = "1";
            String zero = "0"; 

            String[] trip_info = value.toString().split(",");
            int duration = Integer.parseInt( trip_info[0] );

            if (duration == -1){
                String dummyTime = trip_info[1].trim();
                String loc_lat = trip_info[2].trim();
                String loc_long = trip_info[3].trim();

                try {
                    Date aDate = aDateFormat.parse(dummyTime);

                    context.write(new Text(new SimpleDateFormat("EEE").format( aDate ) + ","
                                + new SimpleDateFormat("MMM").format( aDate )  + ","
                                + new SimpleDateFormat("dd").format( aDate ) + ","
                                + new SimpleDateFormat("yyyy").format( aDate ) + ","
                                + new SimpleDateFormat("HH").format( aDate )  + ","
                                + loc_lat + ","
                                + loc_long), new Text( zero + "," + zero ) );

                } catch (ParseException pe) {

                }

            }else{

                String start_time = trip_info[1];
                String end_time = trip_info[2];
                String start_lat = trip_info[5];
                String start_long = trip_info[6];
                String end_lat = trip_info[9];
                String end_long = trip_info[10];
                String user_type = trip_info[12];

                if ( user_type.equals("Subscriber") ){

                    try {
                        Date aDate = aDateFormat.parse(start_time);

                        context.write(new Text(new SimpleDateFormat("EEE").format( aDate ) + ","
                                    + new SimpleDateFormat("MMM").format( aDate )  + ","
                                    + new SimpleDateFormat("dd").format( aDate ) + ","
                                    + new SimpleDateFormat("yyyy").format( aDate ) + ","
                                    + new SimpleDateFormat("HH").format( aDate )  + ","
                                    + start_lat + ","
                                    + start_long), new Text( zero + "," + one ) );
                    
                        aDate = aDateFormat.parse(end_time);

                        context.write(new Text(new SimpleDateFormat("EEE").format( aDate ) + ","
                                    + new SimpleDateFormat("MMM").format( aDate )  + ","
                                    + new SimpleDateFormat("dd").format( aDate ) + ","
                                    + new SimpleDateFormat("yyyy").format( aDate ) + ","
                                    + new SimpleDateFormat("HH").format( aDate )  + ","
                                    + end_lat + ","
                                    + end_long), new Text( one + "," + zero ) );
                    
                    } catch (ParseException pe) {
                    
                    }

                }

            }

		}
	}

	public static class timeWindowReducer
	  extends Reducer<Text,Text,Text,Text> {

		public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

            int sum_start = 0;
            int sum_end = 0;
			for (Text val : values) {
                String[] counts = val.toString().split(",");
                sum_start += Integer.parseInt( counts[0].toString() );
                sum_end += Integer.parseInt( counts[1].toString() );
			}
            context.write(new Text(key.toString().trim() + "," + Integer.toString( sum_start ) + "," + Integer.toString( sum_end ) ), null);

		}

	}

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "timeWindow");

    job.setJarByClass(timeWindow.class);

    job.setMapperClass(timeWindowMapper.class);

    //job.setCombinerClass(timeWindowReducer.class);

    job.setReducerClass(timeWindowReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
