package ca.uwaterloo.cs451.a1;

import com.google.common.io.Files;
import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfStrings;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class PairsPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PairsPMI.class);
    protected int lineCount = 1;

    protected List<String> readLines(String src) {
        List<String> lines = Collections.emptyList();
        try {
            lines = Files.readLines(new File(src), StandardCharsets.UTF_8);
        } catch (IOException e) {
            LOG.error("Error finding file", e);
        }
        return lines;
    }

    /**
     * The main job
     **/

    private static final class PairsPMIMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
        private static final PairOfStrings PAIR_OF_STRINGS = new PairOfStrings();
        private static final IntWritable LINE_NUMBER = new IntWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            context.getConfiguration().setInt("lineCount", 1);
        }

        @Override
        public void map(LongWritable key, Text value, Context context)  //TODO::check if each call of map is a new line
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());
            int lineNumber = context.getConfiguration().getInt("lineCount", 1);

            for (int i = 0; i < 40; i++) {

                PAIR_OF_STRINGS.set(tokens.get(i), "*");
                LINE_NUMBER.set(lineNumber);
                context.write(PAIR_OF_STRINGS, LINE_NUMBER);

                for (int j = i + 1; j < 40; j++) {
                    if (i == j) continue;
                    PAIR_OF_STRINGS.set(tokens.get(i), tokens.get(j));
                    LINE_NUMBER.set(lineNumber);
                    context.write(PAIR_OF_STRINGS, LINE_NUMBER);
                }

            }

            lineNumber += 1;
            context.getConfiguration().setInt("lineCount", lineNumber);
        }

    }

    private static final class PairsPMICombiner extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
        @Override
        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            List<IntWritable> addedInts = Collections.emptyList();
            for (IntWritable value : values) {
                if (addedInts.contains(value)) continue;
                addedInts.add(value);
                context.write(key, value);
            }
        }
    }

//    private static final class PairPmiReducer extends
//            Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
//
//        @Override
//        public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context)
//                throws IOException, InterruptedException {
//            List<IntWritable> addedInts = Collections.emptyList();
//            for (IntWritable value : values) {
//                if (addedInts.contains(value)) continue;
//                addedInts.add(value);
//                context.write(key, value);
//            }
//        }
//    }


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

        @Option(name = "-window", metaVar = "[num]", usage = "cooccurrence window")
        int window = 2;
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
        LOG.info(" - window: " + args.window);
        LOG.info(" - number of reducers: " + args.numReducers);

        Job job = Job.getInstance(getConf());
        job.setJobName(PairsPMI.class.getSimpleName());
        job.setJarByClass(PairsPMI.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);

        job.getConfiguration().setInt("window", args.window);

        job.setNumReduceTasks(args.numReducers);

        FileInputFormat.setInputPaths(job, new Path(args.input));
        FileOutputFormat.setOutputPath(job, new Path(args.output));

        job.setMapOutputKeyClass(PairOfStrings.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(PairOfStrings.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(PairsPMIMapper.class);
        //job.setCombinerClass(MyReducer.class);
        job.setReducerClass(PairsPMICombiner.class);
        job.setPartitionerClass(MyPartitioner.class);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

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