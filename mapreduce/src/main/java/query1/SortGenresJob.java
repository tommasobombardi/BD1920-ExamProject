package query1;

import common.ParserUtility;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SortGenresJob {

    public static class SortGenresMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text genre = new Text();
        private IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (!value.toString().equals(ParserUtility.noGenresListed)) {
                String[] genres = ParserUtility.splitPipe(value.toString());
                for (String g: genres) {
                    genre.set(g);
                    context.write(genre, one);
                }
            }
        }
    }

    public static class SortGenresCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable genreCount = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val: values) {
                count += val.get();
            }
            genreCount.set(count);
            context.write(key, genreCount);
        }
    }

    public static class SortGenresReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text genre = new Text();
        private IntWritable genreCount = new IntWritable();
        private Map<String, Integer> movieGenres = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
            int count = 0;
            for (IntWritable val: values) {
                count += val.get();
            }
            movieGenres.put(key.toString(), count);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<String, Integer>> mg = new LinkedList<>(movieGenres.entrySet());
            Collections.sort(mg, new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue() - o1.getValue();
                }
            });
            for (Map.Entry<String, Integer> g : mg) {
                genre.set(g.getKey());
                genreCount.set(g.getValue());
                context.write(genre, genreCount);
            }
        }
    }

}
