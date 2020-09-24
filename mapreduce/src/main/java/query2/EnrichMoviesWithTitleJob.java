package query2;

import common.ParserUtility;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.PatternSyntaxException;

public class EnrichMoviesWithTitleJob {

    public static class JoinFeelingsMapper extends Mapper<IntWritable, Text, LongWritable, Text> {
        private LongWritable movieId = new LongWritable();
        private Text flagYearCount = new Text();

        @Override
        protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] v = ParserUtility.splitPipe(value.toString());
            long id = Long.parseLong(v[0]);
            int count = Integer.parseInt(v[1]);
            movieId.set(id);
            flagYearCount.set("Feeling" + ParserUtility.slash + key.get() + ParserUtility.pipe + count);
            context.write(movieId, flagYearCount);
        }
    }

    public static class JoinMoviesMapper extends Mapper<Object, Text, LongWritable, Text> {
        private LongWritable movieId = new LongWritable();
        private Text flagTitle = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] line = ParserUtility.splitComma(value.toString());
                long id = Long.parseLong(line[0].trim());
                String title = ParserUtility.getParsedTitle(line[1]);
                movieId.set(id);
                flagTitle.set("Movie" + ParserUtility.slash + title);
                context.write(movieId, flagTitle);
            } catch (NumberFormatException | NullPointerException | PatternSyntaxException ignored) {
                // do nothing in case of error in input parsing (the record will be discarded)
            }
        }
    }

    public static class EnrichMoviesWithTitleReducer extends Reducer<LongWritable, Text, IntWritable, Text> {
        private IntWritable year = new IntWritable();
        private Text titleCount = new Text();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> feelingRecords = new ArrayList<>();
            List<String> movieRecords = new ArrayList<>();
            for(Text v : values) {
                String[] val = ParserUtility.splitSlash(v.toString());
                if (val[0].equals("Feeling")) feelingRecords.add(val[1]);
                else movieRecords.add(val[1]);
            }
            for(String feeling : feelingRecords) {
               for(String movie : movieRecords) {
                    String[] f = ParserUtility.splitPipe(feeling);
                    year.set(Integer.parseInt(f[0]));
                    titleCount.set(movie + ParserUtility.pipe + f[1]);
                    context.write(year, titleCount);
                }
            }
        }
    }

}
