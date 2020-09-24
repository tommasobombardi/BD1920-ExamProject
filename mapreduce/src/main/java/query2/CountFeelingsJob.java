package query2;

import common.ParserUtility;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.PatternSyntaxException;

public class CountFeelingsJob {

    public static class CountRatingsMapper extends Mapper<Text, Text, Text, IntWritable> {
        private IntWritable ratingsCount = new IntWritable();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] v = ParserUtility.splitPipe(value.toString());
            ratingsCount.set(Integer.parseInt(v[1]));
            context.write(key, ratingsCount);
        }
    }

    public static class CountTagsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text idYear = new Text();
        private IntWritable one = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] line = ParserUtility.splitComma(value.toString());
                long movieId = Long.parseLong(line[1].trim());
                int year = ParserUtility.getYearFromTimestamp(line[3]);
                idYear.set(movieId + ParserUtility.pipe + year);
                context.write(idYear, one);
            } catch (NumberFormatException | NullPointerException | PatternSyntaxException ignored) {
                // do nothing in case of error in input parsing (the record will be discarded)
            }
        }
    }

    public static class CountFeelingsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable feelingsCount = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable v: values) {
                count += v.get();
            }
            feelingsCount.set(count);
            context.write(key, feelingsCount);
        }
    }

}
