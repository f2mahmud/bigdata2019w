package ca.uwaterloo.cs451.a4;

import io.bespin.java.mapreduce.pagerank.PageRankNode;
import org.apache.hadoop.io.Writable;
import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.array.ArrayListOfIntsWritable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

public class PersonalizedPageRankNode extends PageRankNode {

    static {
        mapping = new PageRankNode.Type[]{PageRankNode.Type.Complete, PageRankNode.Type.Mass, PageRankNode.Type.Structure};
    }

    private static final PersonalizedPageRankNode.Type[] mapping;
    private ArrayListOfFloatsWritable pageranks;

    public PersonalizedPageRankNode() {
        pageranks = new ArrayListOfFloatsWritable();
    }

    public float getPageRank(int sourceIndex) {
        return pageranks.size() < sourceIndex + 1 ? Float.NEGATIVE_INFINITY : pageranks.get(sourceIndex) ;
    }

    public ArrayListOfFloatsWritable getPageRanks(){
        return pageranks;
    }

    public void setPageRank(int index, float rank) {
        pageranks.set(index,rank);
    }


    /**
     * Deserializes this object.
     *
     * @param in source for raw byte representation
     * @throws IOException if any exception is encountered during object deserialization
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        System.out.println("READDDDINNNGG");
        int b = in.readByte();
        setType(mapping[b]);
        System.out.println("TYPE:::::::" + getType());
        setNodeId(in.readInt());
        System.out.println("NodeId:::::::" + getNodeId());
        if (getType().equals(Type.Mass)) {
            pageranks.readFields(in);
            System.out.println("PageRanks::::" + pageranks);
            return;
        }

        if (getType().equals(Type.Complete)) {
            pageranks.readFields(in);
        }
        System.out.println("PageRanks::::" + pageranks);

        ArrayListOfIntsWritable list = new ArrayListOfIntsWritable();
        list.readFields(in);

        setAdjacencyList(list);
        System.out.println("Neighbours::::" + getAdjacencyList());
    }

    /**
     * Serializes this object.
     *
     * @param out where to write the raw byte representation
     * @throws IOException if any exception is encountered during object serialization
     */
    @Override
    public void write(DataOutput out) throws IOException {
        System.out.println("WRITING");
        out.writeByte(getType().val);
        System.out.println("TYPE:::::::" + getType());
        out.writeInt(getNodeId());
        System.out.println("NodeId:::::::" + getNodeId());
        if (getType().equals(Type.Mass)) {
            pageranks.write(out);
            System.out.println("PageRanks::::" + pageranks);
            return;
        }

        if (getType().equals(Type.Complete)) {
            pageranks.write(out);
        }
        System.out.println("PageRanks::::" + pageranks);
        getAdjacencyList().write(out);
        System.out.println("Neighbours::::" + getAdjacencyList());
    }

    @Override
    public String toString() {
        return String.format("{%d %.5s %s}", getNodeId(), (pageranks == null ? "[ ]" : pageranks.toString()) , (getAdjacencyList() == null ? "[]"
                : getAdjacencyList().toString(10)));
    }

    /**
     * Creates object from a <code>DataInput</code>.
     *
     * @param in source for reading the serialized representation
     * @return newly-created object
     * @throws IOException if any exception is encountered during object deserialization
     */
    public static PersonalizedPageRankNode create(DataInput in) throws IOException {
        PersonalizedPageRankNode m = new PersonalizedPageRankNode();
        m.readFields(in);
        return m;
    }

    /**
     * Creates object from a byte array.
     *
     * @param bytes raw serialized representation
     * @return newly-created object
     * @throws IOException if any exception is encountered during object deserialization
     */
    public static PersonalizedPageRankNode create(byte[] bytes) throws IOException {
        return create(new DataInputStream(new ByteArrayInputStream(bytes)));
    }

}
