package query2;

import common.ParserUtility;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class SortMoviesJob {

    public static class SortMoviesReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private Text sortedMovies = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> movies = new ArrayList<>();
            StringBuilder res = new StringBuilder();
            for (Text v: values) {
                movies.add(v.toString());
            }
            Collections.sort(movies, new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    return Integer.parseInt(ParserUtility.splitPipe(o2)[1]) - Integer.parseInt(ParserUtility.splitPipe(o1)[1]);
                }
            });
            for (String m: movies) {
                res.append(m).append(ParserUtility.doubleSlash);
            }
            sortedMovies.set(res.substring(0, res.length() - 2));
            context.write(key, sortedMovies);
        }
    }

}
