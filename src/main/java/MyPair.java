import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyPair implements WritableComparable<MyPair> {
    private Text first, second;

    public MyPair(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return this.first;
    }

    public Text getSecond() {
        return this.second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public int compareTo(@Nonnull MyPair pair) {
        int a = this.first.compareTo(pair.first);
        if(a == 0) {

            a = this.second.compareTo(pair.second);
        }
        return a;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 29 + second.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MyPair)) {
            return false;
        }
        MyPair tp = (MyPair) obj;
        return first.equals(tp.first) && second.equals(tp.second);
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }
}