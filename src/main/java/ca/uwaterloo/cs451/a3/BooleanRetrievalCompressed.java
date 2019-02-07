package ca.uwaterloo.cs451.a3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import java.io.*;
import java.util.Arrays;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

public class BooleanRetrievalCompressed extends Configured implements Tool {

    private MapFile.Reader[] indexes;
    private FSDataInputStream collection;
    private Stack<Set<Integer>> stack;

    private BooleanRetrievalCompressed() {
    }

    private void initialize(String indexPath, String collectionPath, FileSystem fs) throws IOException {
        File[] folders = new File(indexPath).listFiles();
        indexes = new MapFile.Reader[folders.length];
        int j = 0;
        for (int i = 0; i < folders.length; i++) {
            if (folders[i].isDirectory()) {
                indexes[j] = new MapFile.Reader(new Path(folders[i].toString()), fs.getConf());
                j+=1;
            }
        }
        collection = fs.open(new Path(collectionPath));
        stack = new Stack<>();
    }

    private void runQuery(String q) throws IOException {
        String[] terms = q.split("\\s+");

        for (String t : terms) {
            if (t.equals("AND")) {
                performAND();
            } else if (t.equals("OR")) {
                performOR();
            } else {
                pushTerm(t);
            }
        }

        Set<Integer> set = stack.pop();

        for (Integer i : set) {
            String line = fetchLine(i);
            System.out.println(i + "\t" + line);
        }
    }

    private void pushTerm(String term) throws IOException {
        stack.push(fetchDocumentSet(term));
    }

    private void performAND() {
        Set<Integer> s1 = stack.pop();
        Set<Integer> s2 = stack.pop();

        Set<Integer> sn = new TreeSet<>();

        for (int n : s1) {
            if (s2.contains(n)) {
                sn.add(n);
            }
        }

        stack.push(sn);
    }

    private void performOR() {
        Set<Integer> s1 = stack.pop();
        Set<Integer> s2 = stack.pop();

        Set<Integer> sn = new TreeSet<>();

        for (int n : s1) {
            sn.add(n);
        }

        for (int n : s2) {
            sn.add(n);
        }

        stack.push(sn);
    }

    private Set<Integer> fetchDocumentSet(String term) throws IOException {
        Set<Integer> set = new TreeSet<>();

        ByteArrayInputStream ISTREAM = new ByteArrayInputStream(fetchPostings(term).getBytes());

        DataInputStream INPUT_STREAM = new DataInputStream(ISTREAM);

        int docId = 0;

        while (true) {
            int difference = WritableUtils.readVInt(INPUT_STREAM);
            docId += difference;
            set.add(docId);
            int x = WritableUtils.readVInt(INPUT_STREAM);   //Getting rid of the count thing
            if (x == -1) break;
        }

        INPUT_STREAM.close();

        return set;
    }

    private BytesWritable fetchPostings(String term) throws IOException {

        BytesWritable key = new BytesWritable();
        BytesWritable value = new BytesWritable();
        key.set(term.getBytes(), 0, term.getBytes().length);

        for (int i = 0; i < indexes.length; i++) {
            indexes[i].get(key, value);
            if (value.getBytes().length > 0 && Arrays.equals(key.getBytes(), term.getBytes())) break;
        }

        return value;
    }

    public String fetchLine(long offset) throws IOException {
        collection.seek(offset);
        BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

        String d = reader.readLine();
        return d.length() > 80 ? d.substring(0, 80) + "..." : d;
    }

    private static final class Args {
        @Option(name = "-index", metaVar = "[path]", required = true, usage = "index path")
        String index;

        @Option(name = "-collection", metaVar = "[path]", required = true, usage = "collection path")
        String collection;

        @Option(name = "-query", metaVar = "[term]", required = true, usage = "query")
        String query;
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

        if (args.collection.endsWith(".gz")) {
            System.out.println("gzipped collection is not seekable: use compressed version!");
            return -1;
        }

        FileSystem fs = FileSystem.get(new Configuration());

        initialize(args.index, args.collection, fs);

        System.out.println("Query: " + args.query);
        long startTime = System.currentTimeMillis();
        runQuery(args.query);
        System.out.println("\nquery completed in " + (System.currentTimeMillis() - startTime) + "ms");

        return 1;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     *
     * @param args command-line arguments
     * @throws Exception if tool encounters an exception
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new BooleanRetrievalCompressed(), args);
    }

}