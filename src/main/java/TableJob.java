import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


public class TableJob extends Configured implements Tool {

    static public class MyMapper extends TableMapper<MyPair, Text> {

        @Override
        protected void map(ImmutableBytesWritable rowKey, Result columns, Context context) throws IOException, InterruptedException {
            TableSplit currentSplit = (TableSplit) context.getInputSplit();

            switch ((new String(currentSplit.getTableName()))){
                case Config.INPUT_WEBSITES:
                    Cell robotsInfo = columns.getColumnLatestCell(Bytes.toBytes(Config.CF_INFO), Bytes.toBytes("robots"));

                    String robots = "";
                    if(robotsInfo != null){
                        robots = new String(CellUtil.cloneValue(robotsInfo), StandardCharsets.UTF_8);
                    }

                    Cell siteInfo = columns.getColumnLatestCell(Bytes.toBytes(Config.CF_INFO), Bytes.toBytes("site"));
                    String host = new String(CellUtil.cloneValue(siteInfo), StandardCharsets.UTF_8);

                    context.write(new MyPair(new Text(host), new Text("0")), new Text(robots));
                    break;
                case Config.INPUT_WEBPAGES:
                    String disFlag;
                    Cell url_val = columns.getColumnLatestCell(Bytes.toBytes(Config.CF_DOCS), Bytes.toBytes("url"));
                    String url = new String(CellUtil.cloneValue(url_val), StandardCharsets.UTF_8);

                    url = url.replace("www.", "")
                            .replace("http://", "")
                            .replace("https://", "");
                    host = url.split("/")[0];

                    Cell disabledDocs = columns.getColumnLatestCell(Bytes.toBytes(Config.CF_DOCS), Bytes.toBytes("disabled"));
                    if(disabledDocs != null) {
                        disFlag = Config.YES;
                    } else {
                        disFlag = Config.NO;
                    }

                    String path = "";
                    if(url.split("/").length > 1) {
                        path = url.split("/")[1];
                    }

                    context.write(new MyPair(new Text(host), new Text("1")),
                            new Text(Bytes.toString(rowKey.get()) + "\t" + path + "\t" + disFlag));
                    break;
            }
        }
    }

    static public class MyReducer extends TableReducer<MyPair, Text, ImmutableBytesWritable> {

        Pair<String, String> parseRule(String rule) {
            if(rule.startsWith("/")) {
                return new Pair<>("/", rule);
            } else if(rule.startsWith("*")) {
                return new Pair<>("*", rule.substring(1));
            } else if(rule.endsWith("$")) {
                return new Pair<>("$", rule.substring(0, rule.length()-1));
            } else if(rule.startsWith("/") && rule.endsWith("$")) {
                return new Pair<>("/$", rule.substring(0, rule.length()-1));
            } else if(rule.startsWith("*") && rule.endsWith("$")) {
                return new Pair<>("*$", rule.substring(1, rule.length()-1));
            } else {
                return new Pair<>();
            }
        }

        boolean checkUrl(String url, Pair<String, String> rule) {
            if(rule.getFirst().equals("/") && url.startsWith(rule.getSecond())) {
                return true;
            } else if(rule.getFirst().equals("*") && url.contains(rule.getSecond())) {
                return true;
            } else if(rule.getFirst().equals("$") && url.endsWith(rule.getSecond())) {
                return true;
            } else if(rule.getFirst().equals("/$") && url.equals(rule.getSecond())) {
                return true;
            } else {
                return rule.getFirst().equals("*$") && url.endsWith(rule.getSecond());
            }
        }

        @Override
        protected void reduce(MyPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String robots = values.iterator().next().toString();
            boolean robotsExsits = false;

            List<Pair<String, String>> rules = new ArrayList<>();
            if (robots.startsWith("Disallow:")) {
                robotsExsits = true;
                String[] robotsRules = robots.split("\n");
                for (String rule: robotsRules) {
                    Pair<String, String> ruleTmp = parseRule(rule.split(": ")[1].trim());
                    rules.add(ruleTmp);
                }
            }

            for(Text val: values) {
                String[] parts = val.toString().split("\t");
                String rowKey = parts[0];
                String url = parts[1];
                String disFlag = parts[2];

                boolean isDis = false;
                if(robotsExsits) {
                    for (Pair<String, String> pair : rules) {
                        isDis = checkUrl(url, pair);
                        if (isDis) {
                            break;
                        }
                    }
                }

                if(isDis && disFlag.equals(Config.NO)) {
                    Put put = new Put(Bytes.toBytes(rowKey));
                    put.addColumn(Bytes.toBytes(Config.CF_DOCS), Bytes.toBytes("disabled"), Bytes.toBytes(Config.YES));
                    context.write(null, put);
                } else if(!isDis && disFlag.equals(Config.YES)) {
                    Delete del = new Delete(Bytes.toBytes(rowKey));
                    del.addColumn(Bytes.toBytes(Config.CF_DOCS), Bytes.toBytes("disabled"));
                    context.write(null, del);
                }
            }
        }
    }

    public static class MyPartitioner extends Partitioner<MyPair, NullWritable> {
        @Override
        public int getPartition(MyPair key, NullWritable value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    public static class MySortComparator extends WritableComparator {
        protected MySortComparator() {
            super(MyPair.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            return ((MyPair) w1).compareTo((MyPair) w2);
        }
    }

    public static class MyGroupComparator extends WritableComparator {
        protected MyGroupComparator() {
            super(MyPair.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            return ((MyPair) w1).getFirst().compareTo(((MyPair) w2).getFirst());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    Job GetJobConf(String webpages, String websites) throws IOException {
        Job job = Job.getInstance(getConf(), TableJob.class.getCanonicalName());
        job.setJarByClass(TableJob.class);

        job.setPartitionerClass(MyPartitioner.class);
        job.setSortComparatorClass(MySortComparator.class);
        job.setGroupingComparatorClass(MyGroupComparator.class);

        List<Scan> scans = new ArrayList<>();
        Scan scan1 = new Scan();
        scan1.addColumn(Bytes.toBytes(Config.CF_DOCS), Bytes.toBytes("url"));
        scan1.addColumn(Bytes.toBytes(Config.CF_DOCS), Bytes.toBytes("disabled"));
        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(webpages));
        scans.add(scan1);

        Scan scan2 = new Scan();
        scan2.addColumn(Bytes.toBytes(Config.CF_INFO), Bytes.toBytes("site"));
        scan2.addColumn(Bytes.toBytes(Config.CF_INFO), Bytes.toBytes("robots"));
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(websites));
        scans.add(scan2);

        TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, MyPair.class, Text.class, job, true);

        job.setMapOutputKeyClass(MyPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(Config.COUNT_REDUCERS);

        TableMapReduceUtil.initTableReducerJob(Config.OUTPUT_WEBPAGES, MyReducer.class, job);

        return job;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(HBaseConfiguration.create(), new TableJob(), args));
    }
}