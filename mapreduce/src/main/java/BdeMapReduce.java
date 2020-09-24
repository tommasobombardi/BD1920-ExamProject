import common.JobFactory;
import query1.EnrichMoviesWithGenreJob.JoinGenresMapper;
import query1.EnrichMoviesWithGenreJob.JoinRatingsMapper;
import query1.EnrichMoviesWithGenreJob.EnrichMoviesWithGenreReducer;
import query1.SortGenresJob.SortGenresMapper;
import query1.SortGenresJob.SortGenresCombiner;
import query1.SortGenresJob.SortGenresReducer;
import query1.AggregateRatingsJob.AggregateRatingsMapper;
import query1.AggregateRatingsJob.AggregateRatingsReducer;
import query1.AvgMovieRatingsJob.AvgMovieRatingsCombiner;
import query1.AvgMovieRatingsJob.AvgMovieRatingsMapper;
import query1.AvgMovieRatingsJob.AvgMovieRatingsReducer;
import query2.EnrichMoviesWithTitleJob.JoinFeelingsMapper;
import query2.EnrichMoviesWithTitleJob.JoinMoviesMapper;
import query2.EnrichMoviesWithTitleJob.EnrichMoviesWithTitleReducer;
import query2.CountFeelingsJob.CountRatingsMapper;
import query2.CountFeelingsJob.CountTagsMapper;
import query2.CountFeelingsJob.CountFeelingsReducer;
import query2.SortMoviesJob.SortMoviesReducer;
import query2.FindTopMoviesJob.FindTopMoviesMapper;
import query2.FindTopMoviesJob.FindTopMoviesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.ArrayList;
import java.util.List;

public class BdeMapReduce {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        List<Job> jobs = new ArrayList<>();

        Path inputMoviesPath = new Path(args[0]);
        Path inputRatingsPath = new Path(args[1]);
        Path inputTagsPath = new Path(args[2]);
        Path[] outputPath = new Path[8];
        String currentQuery = "/query1";
        for (int i = 0; i < 8; i++) {       // delete output paths if they already exist
            if (i == 4) currentQuery = "/query2";
            outputPath[i] = new Path(args[3] + currentQuery, "output" + (i + 1));
            if (fs.exists(outputPath[i])) fs.delete(outputPath[i], true);
        }
        conf.setDouble("thresold", Double.parseDouble(args[4]));    // parameter for query 1
        conf.setInt("moviesNumber", Integer.parseInt(args[5]));     // parameter for query 2

        Integer[] numReducers = new Integer[6];
        for (int i = 0; i < 6; i++) {   // num reducers not set with 0 or missing argument
            if (args.length >= i + 7 && Integer.parseInt(args[i + 6]) > 0) numReducers[i] = Integer.parseInt(args[i + 6]);
            else numReducers[i] = null;
        }

        // QUERY 1
        jobs.add(JobFactory.createSingleInputJob("Aggregate Ratings", conf, BdeMapReduce.class,
                AggregateRatingsMapper.class, AggregateRatingsReducer.class, AggregateRatingsReducer.class,
                numReducers[0], Text.class, Text.class, Text.class, Text.class,
                TextInputFormat.class, SequenceFileOutputFormat.class, inputRatingsPath, outputPath[0]));

        jobs.add(JobFactory.createSingleInputJob("Avg Movie Ratings", conf, BdeMapReduce.class,
                AvgMovieRatingsMapper.class, AvgMovieRatingsCombiner.class, AvgMovieRatingsReducer.class,
                numReducers[1], LongWritable.class, Text.class, LongWritable.class, DoubleWritable.class,
                SequenceFileInputFormat.class, SequenceFileOutputFormat.class, outputPath[0], outputPath[1]));

        jobs.add(JobFactory.createDoubleInputJob("Enrich Movies With Genre", conf, BdeMapReduce.class,
                JoinGenresMapper.class, JoinRatingsMapper.class, null, EnrichMoviesWithGenreReducer.class,
                numReducers[2], LongWritable.class, Text.class, LongWritable.class, Text.class,
                TextInputFormat.class, SequenceFileInputFormat.class, SequenceFileOutputFormat.class, inputMoviesPath, outputPath[1], outputPath[2]));

        jobs.add(JobFactory.createSingleInputJob("Sort Genres", conf, BdeMapReduce.class,
                SortGenresMapper.class, SortGenresCombiner.class, SortGenresReducer.class,
                1, Text.class, IntWritable.class, Text.class, IntWritable.class,
                SequenceFileInputFormat.class, TextOutputFormat.class, outputPath[2], outputPath[3]));

        // QUERY 2
        jobs.add(JobFactory.createDoubleInputJob("Count Feelings", conf, BdeMapReduce.class,
                CountRatingsMapper.class, CountTagsMapper.class, CountFeelingsReducer.class, CountFeelingsReducer.class,
                numReducers[3], Text.class, IntWritable.class, Text.class, IntWritable.class,
                SequenceFileInputFormat.class, TextInputFormat.class, SequenceFileOutputFormat.class, outputPath[0], inputTagsPath, outputPath[4]));

        jobs.add(JobFactory.createSingleInputJob("Find Top Movies", conf, BdeMapReduce.class,
                FindTopMoviesMapper.class, FindTopMoviesReducer.class, FindTopMoviesReducer.class,
                numReducers[4], IntWritable.class, Text.class, IntWritable.class, Text.class,
                SequenceFileInputFormat.class, SequenceFileOutputFormat.class, outputPath[4], outputPath[5]));

        jobs.add(JobFactory.createDoubleInputJob("Enrich Movies With Title", conf, BdeMapReduce.class,
                JoinFeelingsMapper.class, JoinMoviesMapper.class, null,  EnrichMoviesWithTitleReducer.class,
                numReducers[5], LongWritable.class, Text.class, IntWritable.class, Text.class,
                SequenceFileInputFormat.class, TextInputFormat.class, SequenceFileOutputFormat.class, outputPath[5], inputMoviesPath, outputPath[6]));

        jobs.add(JobFactory.createSingleInputJob("Sort Movies", conf, BdeMapReduce.class,
                null, null, SortMoviesReducer.class,
                1, IntWritable.class, Text.class, IntWritable.class, Text.class,
                SequenceFileInputFormat.class, TextOutputFormat.class, outputPath[6], outputPath[7]));

        for (Job j: jobs) {
            if (!j.waitForCompletion(true)) System.exit(1);
        }
    }

}
