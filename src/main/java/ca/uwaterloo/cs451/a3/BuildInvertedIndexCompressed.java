package ca.uwaterloo.cs451.a3;

import io.bespin.java.util.Tokenizer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfInts;
import tl.lin.data.pair.PairOfObjectInt;
import tl.lin.data.pair.PairOfStringInt;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class BuildInvertedIndexCompressed extends Configured implements Tool {

    private static final Logger LOG = Logger.getLogger(BuildInvertedIndexCompressed.class);

    private static final class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, PairOfInts> {

        private static final PairOfInts COUNT = new PairOfInts();
        private static final Object2IntFrequencyDistribution<String> COUNTS =
                new Object2IntFrequencyDistributionEntry<>();

        private static final PairOfStringInt KEY = new PairOfStringInt();

        @Override
        public void map(LongWritable docno, Text doc, Context context)
                throws IOException, InterruptedException {
            List<String> tokens = Tokenizer.tokenize(doc.toString());

            // Build a histogram of the terms.
            COUNTS.clear();
            for (String token : tokens) {
                COUNTS.increment(token);
            }

            // Emit postings.
            for (PairOfObjectInt<String> e : COUNTS) {
                KEY.set(e.getLeftElement(), (int) docno.get());
                COUNT.set((int)docno.get(), e.getRightElement());
                context.write(KEY, COUNT);
            }

        }

    }

    public static class MyPartitioner extends Partitioner<PairOfStringInt, PairOfInts> {

        @Override
        public int getPartition(PairOfStringInt pairOfStringInt, PairOfInts pairOfInts, int i) {
            return Math.abs(pairOfStringInt.getLeftElement().hashCode() % i);
        }

    }

    public static class MyGroupingComparator extends WritableComparator {

        public MyGroupingComparator() {
            super(PairOfStringInt.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            PairOfStringInt p1 = (PairOfStringInt) a;
            PairOfStringInt p2 = (PairOfStringInt) b;
            return p1.getLeftElement().compareTo(p2.getLeftElement());
        }
    }

    private static final class MyReducer extends
            Reducer<PairOfStringInt, PairOfInts, Text, BytesWritable> {

        private static final Text KEY = new Text();
        private static final BytesWritable VALUE = new BytesWritable();     //Stores(doc, null, count)

        private static PairOfInts PAIR_OF_INTS = new PairOfInts();  //docno, count

        private static final ByteArrayOutputStream BSTREAM = new ByteArrayOutputStream();
        private static final DataOutputStream DATA_OUTPUT_STREAM = new DataOutputStream(BSTREAM);

        @Override
        public void reduce(PairOfStringInt key, Iterable<PairOfInts> values, Context context)
                throws IOException, InterruptedException {

            BSTREAM.reset();

            KEY.set(key.getLeftElement());

            Iterator<PairOfInts> iter = values.iterator();
            int previousDocCount = 0 ;
            int currentDocCount;

            while (iter.hasNext()) {
                PAIR_OF_INTS = iter.next();
                currentDocCount = PAIR_OF_INTS.getLeftElement();
                WritableUtils.writeVInt(DATA_OUTPUT_STREAM,currentDocCount - previousDocCount);
                WritableUtils.writeVInt(DATA_OUTPUT_STREAM, PAIR_OF_INTS.getRightElement());
                previousDocCount = currentDocCount;
            }

            VALUE.set(BSTREAM.toByteArray(),0,BSTREAM.size());
            context.write(KEY, VALUE);

        }


    }

    private BuildInvertedIndexCompressed() {
    }

    private static final class Args {
        @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
        String input;

        @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
        String output;

        @Option(name = "-reducers", required = false)
        int reducers = 1;
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

        LOG.info("Tool: " + BuildInvertedIndexCompressed.class.getSimpleName());
        LOG.info(" - input path: " + args.input);
        LOG.info(" - output path: " + args.output);
        LOG.info("- number of reducers: " + args.reducers);

        Job job = Job.getInstance(getConf());
        job.setJobName(BuildInvertedIndexCompressed.class.getSimpleName());
        job.setJarByClass(BuildInvertedIndexCompressed.class);

        job.setNumReduceTasks(args.reducers);

        FileInputFormat.setInputPaths(job, new Path(args.input));
        FileOutputFormat.setOutputPath(job, new Path(args.output));

        job.setMapOutputKeyClass(PairOfStringInt.class);
        job.setMapOutputValueClass(PairOfInts.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setOutputFormatClass(MapFileOutputFormat.class);

        job.setMapperClass(MyMapper.class);
        job.setPartitionerClass(MyPartitioner.class);
        job.setGroupingComparatorClass(MyGroupingComparator.class);
        job.setReducerClass(MyReducer.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path(args.output);
        FileSystem.get(getConf()).delete(outputDir, true);

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
        ToolRunner.run(new BuildInvertedIndexCompressed(), args);
    }

}


