package query1;

import common.ParserUtility;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AvgMovieRatingsJob {

    public static class AvgMovieRatingsMapper extends Mapper<Text, Text, LongWritable, Text> {
        private LongWritable movieId = new LongWritable();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] k = ParserUtility.splitPipe(key.toString());
            movieId.set(Long.parseLong(k[0]));
            context.write(movieId, value);
        }
    }

    public static class AvgMovieRatingsCombiner extends Reducer<LongWritable, Text, LongWritable, Text> {
        private Text sumCount = new Text();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (Text v: values) {
                String[] val = ParserUtility.splitPipe(v.toString());
                sum += Double.parseDouble(val[0]);
                count += Integer.parseInt(val[1]);
            }
            sumCount.set(sum + ParserUtility.pipe + count);
            context.write(key, sumCount);
        }
    }

    public static class AvgMovieRatingsReducer extends Reducer<LongWritable, Text, LongWritable, DoubleWritable> {
        private DoubleWritable avg = new DoubleWritable();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (Text v: values) {
                String[] val = ParserUtility.splitPipe(v.toString());
                sum += Double.parseDouble(val[0]);
                count += Integer.parseInt(val[1]);
            }
            avg.set(sum / count);
            context.write(key, avg);
        }
    }

}
