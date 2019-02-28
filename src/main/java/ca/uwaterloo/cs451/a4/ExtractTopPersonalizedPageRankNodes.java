package ca.uwaterloo.cs451.a4;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import scala.xml.Null;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectFloat;
import tl.lin.data.queue.TopScoredObjects;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class ExtractTopPersonalizedPageRankNodes extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(ExtractTopPersonalizedPageRankNodes.class);

    private static class MyMapper extends
            Mapper<IntWritable, PersonalizedPageRankNode, PairOfInts, FloatWritable> {
        private List<TopScoredObjects<Integer>> queues;
        private List<Integer> sources;

        @Override
        public void setup(Context context) throws IOException {

            int k = context.getConfiguration().getInt("top", 100);
            queues = new ArrayList<>();
            sources = new ArrayList<>();
            for (String i : context.getConfiguration().get(SOURCES_STRING).split(",")) {
                sources.add(Integer.parseInt(i));
                queues.add(new TopScoredObjects<>(k));
            }

        }

        @Override
        public void map(IntWritable nid, PersonalizedPageRankNode node, Context context) throws IOException,
                InterruptedException {
            System.out.println(">>>>>>>>>>>>>>>>>node id  " + node);
            for (int i = 0; i < sources.size(); i++) {
                queues.get(i).add(node.getNodeId(), node.getPageRank(i));
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            PairOfInts key = new PairOfInts();
            FloatWritable value = new FloatWritable();

            for (int i = 0; i < queues.size(); i++) {
                for (PairOfObjectFloat<Integer> pair : queues.get(i).extractAll()) {
                    key.set(sources.get(i), pair.getLeftElement());
                    value.set(pair.getRightElement());
                    context.write(key, value);
                }
            }

        }

    }

    private static class MyReducer extends
            Reducer<PairOfInts, FloatWritable, Text, NullWritable> {
        private List<TopScoredObjects<Integer>> queues;
        private List<Integer> sources;

        @Override
        public void setup(Context context) throws IOException {

            int k = context.getConfiguration().getInt("top", 100);
            queues = new ArrayList<>();
            sources = new ArrayList<>();
            for (String i : context.getConfiguration().get(SOURCES_STRING).split(",")) {
                sources.add(Integer.parseInt(i));
                queues.add(new TopScoredObjects<>(k));
            }

        }

        @Override
        public void reduce(PairOfInts nid, Iterable<FloatWritable> iterable, Context context)
                throws IOException {
            int sourceIndex = sources.indexOf(nid.getLeftElement());
            Iterator<FloatWritable> iter = iterable.iterator();
            queues.get(sourceIndex).add(nid.getRightElement(), iter.next().get());

            // Shouldn't happen. Throw an exception.
            if (iter.hasNext()) {
                throw new RuntimeException();
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            Text key = new Text();

            for (int i = 0; i < queues.size(); i++) {
                key.set("Source: " + sources.get(i));
                context.write(key, NullWritable.get());
                for (PairOfObjectFloat<Integer> pair : queues.get(i).extractAll()) {
                    key.set(String.format("%.5f %d", Math.exp(pair.getRightElement()), pair.getLeftElement()));
                    // We're outputting a string so we can control the formatting.
                    context.write(key, NullWritable.get());
                }
            }

        }
    }

    public ExtractTopPersonalizedPageRankNodes() {
    }

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String TOP = "top";
    private static final String SOURCES_STRING = "sources";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({"static-access"})
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("top n").create(TOP));
        options.addOption(OptionBuilder.withArgName("nums").hasArg()
                .withDescription("sources").create(SOURCES_STRING));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(TOP)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int top = Integer.parseInt(cmdline.getOptionValue(TOP));
        //String[] sources = cmdline.getOptionValue(SOURCES_STRING).split(",");

        LOG.info("Tool name: " + ExtractTopPersonalizedPageRankNodes.class.getSimpleName());
        LOG.info(" - input: " + inputPath);
        LOG.info(" - output: " + outputPath);
        LOG.info(" - top: " + top);

        Configuration conf = getConf();
        conf.setInt("mapred.min.split.size", 1024 * 1024 * 1024);
        conf.setInt("top", top);
        conf.set(SOURCES_STRING, cmdline.getOptionValue(SOURCES_STRING));

        Job job = Job.getInstance(conf);
        job.setJobName(ExtractTopPersonalizedPageRankNodes.class.getName() + ":" + inputPath);
        job.setJarByClass(ExtractTopPersonalizedPageRankNodes.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(PairOfInts.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        // Text instead of FloatWritable so we can control formatting

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Delete the output directory if it exists already.
        FileSystem.get(conf).delete(new Path(outputPath), true);

        job.waitForCompletion(true);

        FileSystem fs = FileSystem.get(getConf());
        try (FSDataInputStream fin = fs.open(new Path(outputPath + "/part-r-00000"));
        ) {
            LineReader reader = new LineReader(fin.getWrappedStream());
            Text line = new Text();
            while (true) {
                reader.readLine(line);
                System.out.println(line);

            }
        } catch (EOFException e) {
        }


        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ExtractTopPersonalizedPageRankNodes(), args);
        System.exit(res);
    }


}
