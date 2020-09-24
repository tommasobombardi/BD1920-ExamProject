package common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

@SuppressWarnings(value = {"rawtypes", "unchecked"})
public class JobFactory {

    public static Job createSingleInputJob(String name, Configuration conf, Class jar, Class mapper, Class combiner, Class reducer,
                                           Integer numReducers, Class mapOutputKey, Class mapOutputValue, Class outputKey, Class outputValue,
                                           Class inputFormat, Class outputFormat, Path inputPath, Path outputPath) throws IOException {
        Job job = Job.getInstance(conf, name);
        if (jar != null) job.setJarByClass(jar);
        if (mapper != null) job.setMapperClass(mapper);
        if (combiner != null) job.setCombinerClass(combiner);
        if (reducer != null) job.setReducerClass(reducer);
        if (numReducers != null) job.setNumReduceTasks(numReducers);
        if (mapOutputKey != null) job.setMapOutputKeyClass(mapOutputKey);
        if (mapOutputValue != null) job.setMapOutputValueClass(mapOutputValue);
        if (outputKey != null) job.setOutputKeyClass(outputKey);
        if (outputValue != null) job.setOutputValueClass(outputValue);
        if (inputFormat != null) job.setInputFormatClass(inputFormat);
        if (outputFormat != null) job.setOutputFormatClass(outputFormat);
        if (inputPath != null) FileInputFormat.addInputPath(job, inputPath);
        if (outputPath != null) FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }

    public static Job createDoubleInputJob(String name, Configuration conf, Class jar, Class mapper1, Class mapper2, Class combiner, Class reducer,
                                           Integer numReducers, Class mapOutputKey, Class mapOutputValue, Class outputKey, Class outputValue,
                                           Class inputFormat1, Class inputFormat2, Class outputFormat, Path inputPath1, Path inputPath2, Path outputPath) throws IOException {
        Job job = Job.getInstance(conf, name);
        if (jar != null) job.setJarByClass(jar);
        if (combiner != null) job.setCombinerClass(combiner);
        if (reducer != null) job.setReducerClass(reducer);
        if (numReducers != null) job.setNumReduceTasks(numReducers);
        if (mapOutputKey != null) job.setMapOutputKeyClass(mapOutputKey);
        if (mapOutputValue != null) job.setMapOutputValueClass(mapOutputValue);
        if (outputKey != null) job.setOutputKeyClass(outputKey);
        if (outputValue != null) job.setOutputValueClass(outputValue);
        if (outputFormat != null) job.setOutputFormatClass(outputFormat);
        if (inputPath1 != null && inputFormat1 != null && mapper1 != null) MultipleInputs.addInputPath(job, inputPath1, inputFormat1, mapper1);
        if (inputPath2 != null && inputFormat2 != null && mapper2 != null) MultipleInputs.addInputPath(job, inputPath2, inputFormat2, mapper2);
        if (outputPath != null) FileOutputFormat.setOutputPath(job, outputPath);
        return job;
    }

}
