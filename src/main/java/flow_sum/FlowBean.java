package flow_sum;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author tony.chenjy
 * @date 2019-03-07
 */
public class FlowBean implements WritableComparable<FlowBean> {

    private long download_sum;
    private long upload_sum;
    private long total_sum;

    public FlowBean() {}

    public FlowBean(long download_sum, long upload_sum) {
        this.setFlow(download_sum, upload_sum);
    }

    public void setFlow(long download_sum, long upload_sum) {
        this.download_sum = download_sum;
        this.upload_sum = upload_sum;
        this.total_sum = this.download_sum + this.upload_sum;
    }

    @Override
    public String toString() {
        return this.download_sum + "\t" + this.upload_sum + "\t" + this.total_sum;
    }

    public long getDownload_sum() {
        return download_sum;
    }

    public void setDownload_sum(long download_sum) {
        this.download_sum = download_sum;
    }

    public long getUpload_sum() {
        return upload_sum;
    }

    public void setUpload_sum(long upload_sum) {
        this.upload_sum = upload_sum;
    }

    public long getTotal_sum() {
        return total_sum;
    }

    public void setTotal_sum(long total_sum) {
        this.total_sum = total_sum;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(download_sum);
        dataOutput.writeLong(upload_sum);
        dataOutput.writeLong(total_sum);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.download_sum = dataInput.readLong();
        this.upload_sum = dataInput.readLong();
        this.total_sum = dataInput.readLong();
    }

    @Override
    public int compareTo(FlowBean o) {
        return (int)(o.total_sum - this.total_sum);
    }
}
