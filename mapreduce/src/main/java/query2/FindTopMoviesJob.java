package query2;

import common.ParserUtility;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class FindTopMoviesJob {

    public static class FindTopMoviesMapper extends Mapper<Text, IntWritable, IntWritable, Text> {
        private IntWritable year = new IntWritable();
        private Text idCount = new Text();

        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            String[] k = ParserUtility.splitPipe(key.toString());
            long movieId = Long.parseLong(k[0]);
            int feelingYear = Integer.parseInt(k[1]);
            year.set(feelingYear);
            idCount.set(movieId + ParserUtility.pipe + value.get());
            context.write(year, idCount);
        }
    }

    public static class FindTopMoviesReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private static final int DEFAULT_MOVIE_NUMBER = 10;
        private Text idCount = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int num = context.getConfiguration().getInt("num", DEFAULT_MOVIE_NUMBER);
            List<String> movies = new ArrayList<>();
            for (Text v: values) {
                movies.add(v.toString());
            }
            Collections.sort(movies, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return Integer.parseInt(ParserUtility.splitPipe(o2)[1]) - Integer.parseInt(ParserUtility.splitPipe(o1)[1]);
                }
            });
            for (int i = 0; i < num && i < movies.size(); i++) {
                idCount.set(movies.get(i));
                context.write(key, idCount);
            }
        }
    }

}
