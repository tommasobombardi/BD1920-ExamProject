package query1;

import common.ParserUtility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.PatternSyntaxException;

public class AggregateRatingsJob {

    public static class AggregateRatingsMapper extends Mapper<Object, Text, Text, Text> {
        private Text idYear = new Text();
        private Text sumCount = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] line = ParserUtility.splitComma(value.toString());
                long movieId = Long.parseLong(line[1].trim());
                int year = ParserUtility.getYearFromTimestamp(line[3]);
                double rating = Double.parseDouble(line[2].trim());
                idYear.set(movieId + ParserUtility.pipe + year);
                sumCount.set(rating + ParserUtility.pipe + 1);
                context.write(idYear, sumCount);
            } catch (NumberFormatException | NullPointerException | PatternSyntaxException ignored) {
                // do nothing in case of error in input parsing (the record will be discarded)
            }
        }
    }

    public static class AggregateRatingsReducer extends Reducer<Text, Text, Text, Text> {
        private Text sumCount = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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

}
