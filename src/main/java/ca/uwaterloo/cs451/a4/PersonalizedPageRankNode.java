package ca.uwaterloo.cs451.a4;

import io.bespin.java.mapreduce.pagerank.PageRankNode;
import tl.lin.data.array.ArrayListOfFloatsWritable;
import tl.lin.data.array.ArrayListOfIntsWritable;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
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

    public Float getPageRank(int sourceIndex) {
        return pageranks.size() < sourceIndex + 1 ? Float.NEGATIVE_INFINITY : pageranks.get(sourceIndex) ;
    }

    public ArrayListOfFloatsWritable getPageRanks(){
        return pageranks;
    }

    public void setPageRank(int index, Float rank) {
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
        int b = in.readByte();
        setType(mapping[b]);
        setNodeId(in.readInt());
        if (getType().equals(Type.Mass)) {
            pageranks.readFields(in);
            return;
        }

        if (getType().equals(Type.Complete)) {
            pageranks.readFields(in);
        }

        ArrayListOfIntsWritable list = new ArrayListOfIntsWritable();
        list.readFields(in);

        setAdjacencyList(list);
    }

    /**
     * Serializes this object.
     *
     * @param out where to write the raw byte representation
     * @throws IOException if any exception is encountered during object serialization
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(getType().val);
        out.writeInt(getNodeId());
        if (getType().equals(Type.Mass)) {
            pageranks.write(out);
            return;
        }

        if (getType().equals(Type.Complete)) {
            pageranks.write(out);
        }
        getAdjacencyList().write(out);
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
