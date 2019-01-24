package ca.uwaterloo.cs451.a1;

import com.google.common.io.Files;
import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.pair.PairOfStrings;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.*;

public class PairsPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PairsPMI.class);

    /**
     * For the Occurence Counter Job
     */
    private static class OccurenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static Text KEY = new Text();
        private final static IntWritable ONE = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());
            Set<String> uniqueTokens = new HashSet<String>();
            for (int i = 0; i < 40 && i < tokens.size(); i++) {
                if (uniqueTokens.add(tokens.get(i))) {
                    KEY.set(tokens.get(i));
                    context.write(KEY, ONE);
                }
            }
        }
    }

    private static class OccurenceReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final static IntWritable SUM = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            SUM.set(sum);
            context.write(key, SUM);
        }
    }


    /**
     * The main job
     **/

    private static final class PairsPMIMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
        private static final PairOfStrings PAIR_OF_STRINGS = new PairOfStrings();
        private static final IntWritable ONE = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());
            Set<PairOfStrings> uniqueWordPairs = new HashSet<>();
            for (int i = 0; i < 40 && i < tokens.size(); i++) {
                for (int j = 0; j < 40 && j < tokens.size(); j++) {
                    PAIR_OF_STRINGS.set(tokens.get(i), tokens.get(j));
                    if (i == j || !uniqueWordPairs.add(PAIR_OF_STRINGS)) continue;
                    context.write(PAIR_OF_STRINGS, ONE);
                }
            }
        }

    }

    private static final class PairsPMICombiner extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
        private static final IntWritable SUM = new IntWritable();

        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get() + 1;
            }
            SUM.set(sum);
            context.write(key, SUM);
        }

    }

    private static final class PairsPMIReducer extends
            Reducer<PairOfStrings, IntWritable, PairOfStrings, DoubleWritable> {

        private static final Map<String, Double> occurenceCounts = new HashMap<>();
        private static final DoubleWritable PMI_WRITABLE = new DoubleWritable();

        @Override
        public void setup(Context context) throws IOException {
            List<String> lines;

            try {
                lines = Files.readLines(new File(context.getCacheFiles()[0].toString()), StandardCharsets.UTF_8);
                for (String line : lines) {
                    String[] words = line.split("\\s");
                    occurenceCounts.put(words[0], Double.valueOf(words[1]));
                }
            } catch (IOException e) {
                LOG.error("Error finding file", e);
            }
        }

        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            double numberOfLines = context.getConfiguration().getDouble("numberOfLines", -1);
            int threshold = context.getConfiguration().getInt("threshold", 0);
            System.out.println(">>>>>>>>>Number of lines" + numberOfLines);
            for (IntWritable value : values) {
                sum += value.get();
            }
            if (sum > threshold) {
                double probabilityOfLeft = occurenceCounts.get(key.getLeftElement()) / numberOfLines;
                System.out.println(key.getLeftElement() + "  :::::::::  " + occurenceCounts.get(key.getLeftElement()));
                double probabilityOfRight = occurenceCounts.get(key.getRightElement()) / numberOfLines;
                System.out.println(key.getRightElement() + "  :::::::::  " + occurenceCounts.get(key.getRightElement()));
                double probabilityOfCoccurence = sum / numberOfLines;

                double PMI = Math.log10(probabilityOfCoccurence / (probabilityOfLeft * probabilityOfRight));

                PMI_WRITABLE.set(PMI);
                context.write(key, PMI_WRITABLE);
            }
        }
    }


    private static final class MyPartitioner extends Partitioner<PairOfStrings, IntWritable> {
        @Override
        public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
            return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    /**
     * Creates an instance of this tool.
     */
    private PairsPMI() {
    }

    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        String output;

        @Option(name = "-reducers", metaVar = "[num]", usage = "number of reducers")
        int numReducers = 1;

        @Option(name = "-threshold", metaVar = "[num]", usage = "PMI threshold")
        int threshold = 0;
    }

    /**
     * Runs this tool.
     */
    @Override
    public int run(String[] argv) throws Exception {
        final Args args = new Args();
        CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            return -1;
        }

        LOG.info("Tool: " + PairsPMI.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info(" - threshold: " + args.threshold);
        LOG.info(" - number of reducers: " + args.numReducers);

        Path pathToIntermedieteResults = new Path("./occurenceJobResults/part-r-00000");
        Path pathToIntermedieteResultDirectory = new Path("./occurenceJobResults");
        Path pathToOutputFiles = new Path(args.output);
        Path pathToInputFiles = new Path(args.input);

        /**
         * Running occurence job.
         */
        Job occurenceJob = Job.getInstance(getConf());
        occurenceJob.setJobName("OccurenceCount");
        occurenceJob.setJarByClass(PairsPMI.class);

        FileSystem.get(getConf()).delete(pathToIntermedieteResultDirectory, true);
        FileInputFormat.setInputPaths(occurenceJob, pathToInputFiles);
        FileOutputFormat.setOutputPath(occurenceJob, pathToIntermedieteResultDirectory);

        occurenceJob.setNumReduceTasks(args.numReducers);

        occurenceJob.setMapOutputKeyClass(Text.class);
        occurenceJob.setMapOutputValueClass(IntWritable.class);
        occurenceJob.setOutputKeyClass(Text.class);
        occurenceJob.setOutputValueClass(IntWritable.class);

        occurenceJob.setMapperClass(OccurenceMapper.class);
        occurenceJob.setCombinerClass(OccurenceReducer.class);
        occurenceJob.setReducerClass(OccurenceReducer.class);

        long startTime = System.currentTimeMillis();
        occurenceJob.waitForCompletion(true);
        System.out.println(" Occurence Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        /**
         * Running main job
         */
        Job PairsPMIJob = Job.getInstance(getConf());
        PairsPMIJob.setJobName(PairsPMI.class.getSimpleName());
        PairsPMIJob.setJarByClass(PairsPMI.class);

        PairsPMIJob.addCacheFile(pathToIntermedieteResults.toUri());
        // Delete the output directory if it exists already.
        FileSystem.get(getConf()).delete(pathToOutputFiles, true);

        PairsPMIJob.getConfiguration().setInt("threshold", args.threshold);

        PairsPMIJob.setNumReduceTasks(args.numReducers);

        PairsPMIJob.getConfiguration().setDouble("numberOfLines", java.nio.file.Files.lines(Paths.get(args.input)).count());

        FileInputFormat.setInputPaths(PairsPMIJob, pathToInputFiles);
        FileOutputFormat.setOutputPath(PairsPMIJob, pathToOutputFiles);

        PairsPMIJob.setMapOutputKeyClass(PairOfStrings.class);
        PairsPMIJob.setMapOutputValueClass(IntWritable.class);
        PairsPMIJob.setOutputKeyClass(PairOfStrings.class);
        PairsPMIJob.setOutputValueClass(IntWritable.class);

        PairsPMIJob.setMapperClass(PairsPMIMapper.class);
        PairsPMIJob.setCombinerClass(PairsPMICombiner.class);
        PairsPMIJob.setReducerClass(PairsPMIReducer.class);
        PairsPMIJob.setPartitionerClass(MyPartitioner.class);

        PairsPMIJob.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
        PairsPMIJob.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        PairsPMIJob.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        PairsPMIJob.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        PairsPMIJob.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

        startTime = System.currentTimeMillis();
        PairsPMIJob.waitForCompletion(true);
        System.out.println("PMIPairs Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        //FileSystem.get(getConf()).delete(pathToIntermedieteFiles,true);       //TODO::need to uncomment before submission

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new PairsPMI(), args);
    }
}