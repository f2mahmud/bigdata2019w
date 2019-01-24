package ca.uwaterloo.cs451.a1;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.map.HMapStIW;
import tl.lin.data.map.HashMapWritable;
import tl.lin.data.pair.PairOfWritables;

public class StripesPMI extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(StripesPMI.class);

    /**
     * For the Occurence Counter Job
     */
    private static class OccurenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static Text KEY = new Text();
        private final static IntWritable ONE = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());
            Set<String> uniqueTokens = new HashSet<>();
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
    private static class StripesPMIMapper extends Mapper<LongWritable, Text, Text, HMapStIW> {

        // Objects for reuse
        private static Text KEY = new Text();
        private static HMapStIW MAP = new HMapStIW();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(value.toString());
            String mainKey;
            for (int i = 0; i < 40 && i < tokens.size(); i++) {
                mainKey = tokens.get(i);
                KEY.set(mainKey);
                for (int j = 0; j < 40 && j < tokens.size(); j++) {
                    String valueString = tokens.get(j);
                    if (i == j || MAP.containsKey(valueString)) continue;
                    MAP.put(valueString, 1);
                }
                context.write(KEY, MAP);
                MAP.clear();
            }
        }
    }

    private static class StripesPMICombiner extends Reducer<Text, HMapStIW, Text, HMapStIW> {
        private static HMapStIW MAP = new HMapStIW();

        @Override
        public void reduce(Text key, Iterable<HMapStIW> values, Context context)
                throws IOException, InterruptedException {
            for (HMapStIW valueMap : values) {
                for (String valueKey : valueMap.keySet()) {
                    if (MAP.containsKey(valueKey)) {
                        MAP.put(valueKey, MAP.get(valueKey) + valueMap.get(valueKey));
                    } else {
                        MAP.put(valueKey, valueMap.get(valueKey));
                    }
                }
            }
            context.write(key, MAP);
            MAP.clear();
        }
    }

    private static class StripesPMIReducer extends Reducer<Text, HMapStIW, Text, HashMapWritable> {

        private static Map<String, Integer> MAP = new HashMap<>();
        private static HashMapWritable<Text, PairOfWritables> VALUE = new HashMapWritable<>();

        private static Text TEXT = new Text();
        private static PairOfWritables<DoubleWritable, DoubleWritable> PMI_COCCURENCE = new PairOfWritables<>();

        private static DoubleWritable PMI = new DoubleWritable();
        private static DoubleWritable COCCCURENCE = new DoubleWritable();
        private static final Map<String, Double> occurenceCounts = new HashMap<>();

        private static double numberOfLines;

        @Override
        public void setup(Context context) throws IOException {
            List<String> lines;
            numberOfLines = context.getConfiguration().getDouble("numberOfLines", -1);
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
        public void reduce(Text key, Iterable<HMapStIW> values, Context context)
                throws IOException, InterruptedException {
            for (HMapStIW valueMap : values) {
                for (String valueKey : valueMap.keySet()) {
                    if (MAP.containsKey(valueKey)) {
                        MAP.put(valueKey, MAP.get(valueKey) + valueMap.get(valueKey));
                    } else {
                        MAP.put(valueKey, valueMap.get(valueKey));
                    }
                }
            }
            int threshold = context.getConfiguration().getInt("threshold", 0);
            double probabilityOfLeft;
            double probabilityOfRight;
            double probabilityOfCoccurence;
            for (String valueKey : MAP.keySet()) {
                TEXT.set(valueKey);
                if (MAP.get(valueKey) > threshold && !VALUE.containsKey(TEXT)) {
                    probabilityOfLeft = occurenceCounts.get(key.toString()) / numberOfLines;
                    probabilityOfRight = occurenceCounts.get(valueKey) / numberOfLines;
                    probabilityOfCoccurence = MAP.get(valueKey) / numberOfLines;

                    PMI.set(Math.log10(probabilityOfCoccurence / (probabilityOfLeft * probabilityOfRight)));
                    COCCCURENCE.set(MAP.get(valueKey));
                    PMI_COCCURENCE.set(PMI, COCCCURENCE);
                    VALUE.put(TEXT, PMI_COCCURENCE);
                }
            }
            context.write(key, VALUE);
            VALUE.clear();
            MAP.clear();
        }
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

    public int run(String[] argv) throws Exception {

        long startTime = System.currentTimeMillis();

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
        occurenceJob.setJarByClass(StripesPMI.class);

        FileSystem.get(getConf()).delete(pathToIntermedieteResultDirectory, true);
        FileInputFormat.setInputPaths(occurenceJob, pathToInputFiles);
        TextOutputFormat.setOutputPath(occurenceJob, pathToIntermedieteResultDirectory);

        occurenceJob.setNumReduceTasks(args.numReducers);

        occurenceJob.setMapOutputKeyClass(Text.class);
        occurenceJob.setMapOutputValueClass(IntWritable.class);
        occurenceJob.setOutputKeyClass(Text.class);
        occurenceJob.setOutputValueClass(IntWritable.class);

        occurenceJob.setMapperClass(OccurenceMapper.class);
        occurenceJob.setCombinerClass(OccurenceReducer.class);
        occurenceJob.setReducerClass(OccurenceReducer.class);

        occurenceJob.waitForCompletion(true);

        /**
         * Running main job
         */
        Job StripesPMIJob = Job.getInstance(getConf());
        StripesPMIJob.setJobName(StripesPMI.class.getSimpleName());
        StripesPMIJob.setJarByClass(StripesPMI.class);

        StripesPMIJob.addCacheFile(pathToIntermedieteResults.toUri());
        FileSystem.get(getConf()).delete(pathToOutputFiles, true);

        StripesPMIJob.getConfiguration().setInt("threshold", args.threshold);

        StripesPMIJob.setNumReduceTasks(args.numReducers);

        StripesPMIJob.getConfiguration().setDouble("numberOfLines", java.nio.file.Files.lines(Paths.get(args.input)).count());

        FileInputFormat.setInputPaths(StripesPMIJob, pathToInputFiles);
        TextOutputFormat.setOutputPath(StripesPMIJob, pathToOutputFiles);

        StripesPMIJob.setMapOutputKeyClass(Text.class);
        StripesPMIJob.setMapOutputValueClass(HMapStIW.class);
        StripesPMIJob.setOutputKeyClass(Text.class);
        StripesPMIJob.setOutputValueClass(HashMapWritable.class);

        StripesPMIJob.setMapperClass(StripesPMIMapper.class);
        StripesPMIJob.setCombinerClass(StripesPMICombiner.class);
        StripesPMIJob.setReducerClass(StripesPMIReducer.class);

        StripesPMIJob.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 32);
        StripesPMIJob.getConfiguration().set("mapreduce.map.memory.mb", "3072");
        StripesPMIJob.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
        StripesPMIJob.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
        StripesPMIJob.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

        StripesPMIJob.waitForCompletion(true);
        FileSystem.get(getConf()).delete(pathToIntermedieteResultDirectory, true);

        System.out.println("PMIstripes Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new StripesPMI(), args);
    }
}
