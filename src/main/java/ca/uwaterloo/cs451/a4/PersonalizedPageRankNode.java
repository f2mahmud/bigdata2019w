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

    private ArrayListOfFloatsWritable pageranks;
    private ArrayListOfIntsWritable adjacencyList;

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

    public ArrayListOfIntsWritable getAdjacencyList() {
        return adjacencyList;
    }

    public void setAdjacencyList(ArrayListOfIntsWritable list) {
        this.adjacencyList = list;
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

        if (getType().equals(Type.Mass)) {
            pageranks.readFields(in);
            return;
        }

        if (getType().equals(Type.Complete)) {
            pageranks.readFields(in);
        }

        adjacencyList = new ArrayListOfIntsWritable();
        adjacencyList.readFields(in);
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

        if (getType().equals(Type.Mass)) {
            pageranks.write(out);
            return;
        }

        if (getType().equals(Type.Complete)) {
            pageranks.write(out);
        }

        adjacencyList.write(out);
    }

    @Override
    public String toString() {
        return String.format("{%d %.5s %s}", getNodeId(), (pageranks == null ? "[ ]" : pageranks.toString()) , (adjacencyList == null ? "[]"
                : adjacencyList.toString(10)));
    }

    /**
     * Returns the serialized representation of this object as a byte array.
     *
     * @return byte array representing the serialized representation of this object
     * @throws IOException if any exception is encountered during object serialization
     */
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(bytesOut);
        write(dataOut);

        return bytesOut.toByteArray();
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
