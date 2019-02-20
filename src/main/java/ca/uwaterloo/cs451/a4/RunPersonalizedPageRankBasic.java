package ca.uwaterloo.cs451.a4;

import com.google.common.base.Preconditions;
import io.bespin.java.mapreduce.pagerank.NonSplitableSequenceFileInputFormat;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import tl.lin.data.array.ArrayListOfIntsWritable;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class RunPersonalizedPageRankBasic extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(RunPersonalizedPageRankBasic.class);

    private static enum PageRank {
        nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
    }

    // Mapper, no in-mapper combining.
    private static class MapClass extends
            Mapper<IntWritable, PersonalizedPageRankNode, IntWritable, PersonalizedPageRankNode> {

        // The neighbor to which we're sending messages.
        private static final IntWritable neighbor = new IntWritable();

        // Contents of the messages: partial PageRank mass.
        private static final PersonalizedPageRankNode intermediateMass = new PersonalizedPageRankNode();

        // For passing along node structure.
        private static final PersonalizedPageRankNode intermediateStructure = new PersonalizedPageRankNode();

        @Override
        public void map(IntWritable nid, PersonalizedPageRankNode node, Context context)
                throws IOException, InterruptedException {
            // Pass along node structure.
            intermediateStructure.setNodeId(node.getNodeId());
            intermediateStructure.setType(PersonalizedPageRankNode.Type.Structure);     //adjacency list
            intermediateStructure.setAdjacencyList(node.getAdjacencyList());

            context.write(nid, intermediateStructure);

            int massMessages = 0;

            // Distribute PageRank mass to neighbors (along outgoing edges).
            if (node.getAdjacencyList().size() > 0) {
                // Each neighbor gets an equal share of PageRank mass.
                ArrayListOfIntsWritable list = node.getAdjacencyList();

                for (int i = 0; i < node.getPageRanks().size(); i++) {
                    intermediateMass.setPageRank(i, node.getPageRank(i) - (float) StrictMath.log(list.size()));
                }

                context.getCounter(PageRank.edges).increment(list.size());
                intermediateMass.setType(PersonalizedPageRankNode.Type.Mass);

                // Iterate over neighbors.
                for (int i = 0; i < list.size(); i++) {
                    neighbor.set(list.get(i));
                    intermediateMass.setNodeId(list.get(i));

                    // Emit messages with PageRank mass to neighbors.
                    context.write(neighbor, intermediateMass);
                    massMessages++;
                }
            }

            // Bookkeeping.
            context.getCounter(PageRank.nodes).increment(1);
            context.getCounter(PageRank.massMessages).increment(massMessages);
            System.out.println(nid.get() + " ::::::::::::::: " + massMessages);

        }

        @Override
        public void cleanup(Context context) throws IOException {
            System.out.println("Number of nodes >>>>>>>>>>>>>>>>  " + context.getCounter(PageRank.nodes).getValue());
        }

    }


//    // Combiner: sums partial PageRank contributions and passes node structure along.
//    private static class CombineClass extends
//            Reducer<IntWritable, PersonalizedPageRankNode, IntWritable, PersonalizedPageRankNode> {
//        private static final PersonalizedPageRankNode intermediateMass = new PersonalizedPageRankNode();
//
//        @Override
//        public void reduce(IntWritable nid, Iterable<PersonalizedPageRankNode> values, Context context)
//                throws IOException, InterruptedException {
//            int massMessages = 0;
//
//            // Remember, PageRank mass is stored as a log prob.
//            float mass = Float.NEGATIVE_INFINITY;
//            for (PersonalizedPageRankNode n : values) {
//                if (n.getType() == PersonalizedPageRankNode.Type.Structure) {
//                    // Simply pass along node structure.
//                    context.write(nid, n);
//                } else {
//                    // Accumulate PageRank mass contributions.
//                    mass = sumLogProbs(mass, n.getPageRank());
//                    massMessages++;
//                }
//            }
//
//            // Emit aggregated results.
//            if (massMessages > 0) {
//                intermediateMass.setNodeId(nid.get());
//                intermediateMass.setType(PersonalizedPageRankNode.Type.Mass);
//                intermediateMass.setPageRank(mass);
//
//                context.write(nid, intermediateMass);
//            }
//        }
//    }

    // Reduce: sums incoming PageRank contributions, rewrite graph structure.
    private static class ReduceClass extends
            Reducer<IntWritable, PersonalizedPageRankNode, IntWritable, PersonalizedPageRankNode> {
        // For keeping track of PageRank mass encountered, so we can compute missing PageRank mass lost
        // through dangling nodes.
        private List<Float> totalMasses = new ArrayList<>(); //Float.NEGATIVE_INFINITY;  //TODO::Need to create an array here to take care of multi source

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            for (int i = 0; i < conf.get(SOURCES).split(",").length; i++) {
                totalMasses.add(i, Float.NEGATIVE_INFINITY);
            }
        }

        @Override
        public void reduce(IntWritable nid, Iterable<PersonalizedPageRankNode> iterable, Context context)
                throws IOException, InterruptedException {
            Iterator<PersonalizedPageRankNode> values = iterable.iterator();

            // Create the node structure that we're going to assemble back together from shuffled pieces.
            PersonalizedPageRankNode node = new PersonalizedPageRankNode();

            node.setType(PersonalizedPageRankNode.Type.Complete);
            node.setNodeId(nid.get());

            int massMessagesReceived = 0;
            int structureReceived = 0;

            float mass = Float.NEGATIVE_INFINITY;
            while (values.hasNext()) {
                PersonalizedPageRankNode n = values.next();

                if (n.getType().equals(PersonalizedPageRankNode.Type.Structure)) {
                    // This is the structure; update accordingly.
                    ArrayListOfIntsWritable list = n.getAdjacencyList();
                    structureReceived++;

                    node.setAdjacencyList(list);
                } else {
                    // This is a message that contains PageRank mass; accumulate.
                    for (int i = 0; i < n.getPageRanks().size(); i++) {
                        node.setPageRank(i, sumLogProbs(node.getPageRank(i), n.getPageRank(i)));
                    }
                    massMessagesReceived++;
                }
            }

            // Update the final accumulated PageRank mass.
            context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

            // Error checking.
            if (structureReceived == 1) {
                // Everything checks out, emit final node structure with updated PageRank value.
                context.write(nid, node);

                // Keep track of total PageRank mass.
                for (int i = 0; i < node.getPageRanks().size(); i++) {
                    totalMasses.set(i, sumLogProbs(totalMasses.get(i), node.getPageRank(i)));
                }
            } else if (structureReceived == 0) {
                // We get into this situation if there exists an edge pointing to a node which has no
                // corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
                // log and count but move on.
                context.getCounter(PageRank.missingStructure).increment(1);
                LOG.warn("No structure received for nodeid: " + nid.get() + " mass: "
                        + massMessagesReceived);
                // It's important to note that we don't add the PageRank mass to total... if PageRank mass
                // was sent to a non-existent node, it should simply vanish.
            } else {
                // This shouldn't happen!
                throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
                        + " mass: " + massMessagesReceived + " struct: " + structureReceived);
            }
        }

        @Override
        public void cleanup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            String taskId = conf.get("mapred.task.id");
            String path = conf.get("PageRankMassPath");

            Preconditions.checkNotNull(taskId);
            Preconditions.checkNotNull(path);

            // Write to a file the amount of PageRank mass we've seen in this reducer.
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
            String[] sources = conf.get(SOURCES).split(",");
            for(int i = 0 ; i < sources.length; i++){
                out.writeChars(sources[i]);
                out.writeFloat(totalMasses.get(i));
            }
            out.close();
        }
    }

    // Mapper that distributes the missing PageRank mass (lost at the dangling nodes) and takes care
    // of the random jump factor.
    private static class MapPageRankMassDistributionClass extends
            Mapper<IntWritable, PersonalizedPageRankNode, IntWritable, PersonalizedPageRankNode> {

        private float missingMass = 0.0f;
        private int nodeCnt = 0;
        private static List<Integer> SOURCES_LIST = new ArrayList<>();

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            missingMass = conf.getFloat("MissingMass", 0.0f);
            nodeCnt = conf.getInt("NodeCount", 0);

            for (String source : conf.get(SOURCES).split(",")) {
                SOURCES_LIST.add(Integer.parseInt(source));
            }

        }


        @Override
        public void map(IntWritable nid, PersonalizedPageRankNode node, Context context)
                throws IOException, InterruptedException {
            if (SOURCES_LIST.contains(nid.get())) {
                int index = SOURCES_LIST.indexOf(nid.get());
                float p = node.getPageRank(index);

                float jump = (float) Math.log(ALPHA);  // - Math.log(nodeCnt));
                float link = (float) Math.log(1.0f - ALPHA)
                        + sumLogProbs(p, (float) (Math.log(missingMass))); // - Math.log(nodeCnt)));

                p = sumLogProbs(jump, link);
                node.setPageRank(index, p);
            }

            context.write(nid, node);
        }
    }

    // Random jump factor.
    private static float ALPHA = 0.15f;
    private static NumberFormat formatter = new DecimalFormat("0000");

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new RunPersonalizedPageRankBasic(), args);
    }

    public RunPersonalizedPageRankBasic() {
    }

    private static final String BASE = "base";
    private static final String NUM_NODES = "numNodes";
    private static final String START = "start";
    private static final String END = "end";
    private static final String COMBINER = "useCombiner";
    private static final String INMAPPER_COMBINER = "useInMapperCombiner";
    private static final String RANGE = "range";
    private static final String SOURCES = "sources";

    /**
     * Runs this tool.
     */
    @SuppressWarnings({"static-access"})
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(new Option(COMBINER, "use combiner"));
        options.addOption(new Option(INMAPPER_COMBINER, "user in-mapper combiner"));
        options.addOption(new Option(RANGE, "use range partitioner"));

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("base path").create(BASE));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("start iteration").create(START));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("end iteration").create(END));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of nodes").create(NUM_NODES));
        options.addOption(OptionBuilder.withArgName("sources").hasArg()
                .withDescription("sources").create(SOURCES));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(BASE) || !cmdline.hasOption(START) ||
                !cmdline.hasOption(END) || !cmdline.hasOption(NUM_NODES)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String basePath = cmdline.getOptionValue(BASE);
        String sources = cmdline.getOptionValue(SOURCES);
        int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
        int s = Integer.parseInt(cmdline.getOptionValue(START));
        int e = Integer.parseInt(cmdline.getOptionValue(END));
        boolean useCombiner = cmdline.hasOption(COMBINER);
        boolean useInmapCombiner = cmdline.hasOption(INMAPPER_COMBINER);
        boolean useRange = cmdline.hasOption(RANGE);

        LOG.info("Tool name: RunPageRank");
        LOG.info(" - base path: " + basePath);
        LOG.info(" - num nodes: " + n);
        LOG.info(" - start iteration: " + s);
        LOG.info(" - end iteration: " + e);
        LOG.info(" - use combiner: " + useCombiner);
        LOG.info(" - use in-mapper combiner: " + useInmapCombiner);
        LOG.info(" - user range partitioner: " + useRange);

        // Iterate PageRank.
        for (int i = s; i < e; i++) {
            iteratePageRank(i, i + 1, basePath, n, useCombiner, useInmapCombiner, sources);
        }

        return 0;
    }

    // Run each iteration.
    private void iteratePageRank(int i, int j, String basePath, int numNodes,
                                 boolean useCombiner, boolean useInMapperCombiner, String sources) throws Exception {
        // Each iteration consists of two phases (two MapReduce jobs).

        // Job 1: distribute PageRank mass along outgoing edges.
        float mass = phase1(i, j, basePath, numNodes, useCombiner, useInMapperCombiner, sources);

        // Find out how much PageRank mass got lost at the dangling nodes.
        float missing = 1.0f - (float) StrictMath.exp(mass);

        // Job 2: distribute missing mass, take care of random jump factor.
        phase2(i, j, missing, basePath, numNodes, sources);
    }

    private float phase1(int i, int j, String basePath, int numNodes,
                         boolean useCombiner, boolean useInMapperCombiner, String sources) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("PageRank:Basic:iteration" + j + ":Phase1");
        job.setJarByClass(RunPersonalizedPageRankBasic.class);

        String in = basePath + "/iter" + formatter.format(i);
        String out = basePath + "/iter" + formatter.format(j) + "t";
        String outMassPath = out + "-mass";

        // We need to actually count the number of part files to get the number of partitions (because
        // the directory might contain _log).
        int numPartitions = 0;
        for (FileStatus s : FileSystem.get(getConf()).listStatus(new Path(in))) {
            if (s.getPath().getName().contains("part-"))
                numPartitions++;
        }

        LOG.info("PageRank: iteration " + j + ": Phase1");
        LOG.info(" - input: " + in);
        LOG.info(" - output: " + out);
        LOG.info(" - sources: " + sources);
        LOG.info(" - nodeCnt: " + numNodes);
        LOG.info(" - useCombiner: " + useCombiner);
        LOG.info(" - useInmapCombiner: " + useInMapperCombiner);
        LOG.info("computed number of partitions: " + numPartitions);

        int numReduceTasks = numPartitions;

        job.getConfiguration().setInt("NodeCount", numNodes);
        job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
        //job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
        job.getConfiguration().set("PageRankMassPath", outMassPath);
        job.getConfiguration().set(SOURCES, sources);

        job.setNumReduceTasks(numReduceTasks);

        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PersonalizedPageRankNode.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PersonalizedPageRankNode.class);

        job.setMapperClass(MapClass.class);
//
//        if (useCombiner) {
//            job.setCombinerClass(CombineClass.class);
//        }

        job.setReducerClass(ReduceClass.class);

        FileSystem.get(getConf()).delete(new Path(out), true);
        FileSystem.get(getConf()).delete(new Path(outMassPath), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        float mass = Float.NEGATIVE_INFINITY;
        FileSystem fs = FileSystem.get(getConf());
        for (FileStatus f : fs.listStatus(new Path(outMassPath))) {
            FSDataInputStream fin = fs.open(f.getPath());
            mass = sumLogProbs(mass, fin.readFloat());
            fin.close();
        }

        return mass;
    }

    private void phase2(int i, int j, float missing, String basePath, int numNodes, String sources) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("PageRank:Basic:iteration" + j + ":Phase2");
        job.setJarByClass(RunPersonalizedPageRankBasic.class);

        LOG.info("missing PageRank mass: " + missing);
        LOG.info("sources: " + sources);
        LOG.info("number of nodes: " + numNodes);

        String in = basePath + "/iter" + formatter.format(j) + "t";
        String out = basePath + "/iter" + formatter.format(j);

        LOG.info("PageRank: iteration " + j + ": Phase2");
        LOG.info(" - input: " + in);
        LOG.info(" - output: " + out);

        job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
        job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
        job.getConfiguration().setFloat("MissingMass", (float) missing);
        job.getConfiguration().setInt("NodeCount", numNodes);
        job.getConfiguration().set(SOURCES, sources);

        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));

        job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PersonalizedPageRankNode.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(PersonalizedPageRankNode.class);

        job.setMapperClass(MapPageRankMassDistributionClass.class);

        FileSystem.get(getConf()).delete(new Path(out), true);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
    }

    // Adds two log probs.
    private static float sumLogProbs(float a, float b) {
        if (a == Float.NEGATIVE_INFINITY)
            return b;

        if (b == Float.NEGATIVE_INFINITY)
            return a;

        if (a < b) {
            return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
        }

        return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
    }
}