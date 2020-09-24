package query1;

import common.ParserUtility;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.regex.PatternSyntaxException;

public class EnrichMoviesWithGenreJob {

    public static class JoinGenresMapper extends Mapper<Object, Text, LongWritable, Text> {
        private LongWritable movieId = new LongWritable();
        private Text flagAndGenres = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] line = ParserUtility.splitComma(value.toString());
                long id = Long.parseLong(line[0].trim());
                String genres = line[2].trim();
                movieId.set(id);
                flagAndGenres.set("Genres" + ParserUtility.slash + genres);
                context.write(movieId, flagAndGenres);
            } catch (NumberFormatException | NullPointerException | PatternSyntaxException ignored) {
                // do nothing in case of error in input parsing (the record will be discarded)
            }
        }
    }

    public static class JoinRatingsMapper extends Mapper<LongWritable, DoubleWritable, LongWritable, Text> {
        private static final double DEFAULT_THRESOLD = 3.5;
        private Text flag = new Text("Rating");

        @Override
        protected void map(LongWritable key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            double thresold = context.getConfiguration().getDouble("thresold", DEFAULT_THRESOLD);
            if(value.get() >= thresold) context.write(key, flag);
        }
    }

    public static class EnrichMoviesWithGenreReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        private Text genres = new Text();

        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean satisfiedRating = false;
            String movieGenres = "";
            for (Text val: values) {
                if (val.toString().startsWith("Rating")) satisfiedRating = true;
                else movieGenres = ParserUtility.splitSlash(val.toString())[1];
            }
            if (satisfiedRating) {
                genres.set(movieGenres);
                context.write(key, genres);
            }
        }
    }

}
