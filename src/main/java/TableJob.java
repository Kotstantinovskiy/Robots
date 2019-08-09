import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


public class TableJob extends Configured implements Tool {

    public static class MyMapper extends TableMapper<Text, Text> {

        @Override
        protected void map(ImmutableBytesWritable rowKey, Result columns, Context context) throws IOException, InterruptedException {
            TableSplit currentSplit = (TableSplit) context.getInputSplit();
            switch ((new String(currentSplit.getTableName()))){
                case Config.WEBSITES:
                    byte[] robotsInfo = columns.getValue(Bytes.toBytes(Config.CF_INFO), Bytes.toBytes("robots"));
		    
                    String robots = "";
                    if(robotsInfo != null){
                        robots = new String(robotsInfo, StandardCharsets.UTF_8);
                    }

                    byte[] siteInfo = columns.getValue(Bytes.toBytes(Config.CF_INFO), Bytes.toBytes("site"));
                    String host = new String(siteInfo, StandardCharsets.UTF_8);
		    
                    context.write(new Text(host + Config.DELIMITER + "0"), new Text(robots));
                    break;
                case Config.WEBPAGES:
                    String disFlag;
                    byte[] url_val = columns.getValue(Bytes.toBytes(Config.CF_DOCS), Bytes.toBytes("url"));
                    String url = new String(url_val, StandardCharsets.UTF_8);

                    url = url.replace("http://", "")
                            .replace("https://", "");
                    host = url.split("/")[0];

                    byte[] disabledDocs = columns.getValue(Bytes.toBytes(Config.CF_DOCS), Bytes.toBytes("disabled"));
                    if(disabledDocs != null) {
                        disFlag = Config.YES;
                    } else {
                        disFlag = Config.NO;
                    }

                    String path = "";
                    if(url.split("/").length > 1) {
                        path = "/" + url.split("/", 2)[1];
                    }

                    context.write(new Text(host + Config.DELIMITER + "1"),
                            new Text(Bytes.toString(rowKey.get()) + "\t" + path + "\t" + disFlag));
                    break;
            }
        }
    }

    public static class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
        List<Pair<String, String>> rules = new ArrayList<>();
        boolean robotsExsits = false;

        Pair<String, String> parseRule(String rule) {
            if(rule.startsWith("/") && rule.endsWith("$")) {
                return new Pair<>("/$", rule.substring(0, rule.length()-1));
            } else if(rule.startsWith("*") && rule.endsWith("$")) {
                return new Pair<>("*$", rule.substring(1, rule.length()-1));
            } else if(rule.startsWith("/")) {
                return new Pair<>("/", rule);
            } else if(rule.startsWith("*")) {
                return new Pair<>("*", rule.substring(1));
            } else if(rule.endsWith("$")) {
                return new Pair<>("$", rule.substring(0, rule.length()-1));
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
            } else if(rule.getFirst().equals("*$") && url.endsWith(rule.getSecond())) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if(key.toString().split(Config.DELIMITER)[1].equals("0")) {
                String robots = values.iterator().next().toString();
                rules.clear();
		
                if (robots.startsWith("Disallow:")) {
		    
                    robotsExsits = true;
                    String[] robotsRules = robots.split("\n");
                    for (String rule : robotsRules) {
                        Pair<String, String> ruleTmp = parseRule(rule.split(":", 2)[1].trim());
			System.out.println(ruleTmp.getFirst() + " " + ruleTmp.getSecond());
                        rules.add(ruleTmp);
                    }
                }
            } else {
                for (Text val : values) {
                    String[] parts = val.toString().split("\t");
                    String rowKey = parts[0];
                    String url = parts[1];
                    String disFlag = parts[2];

                    boolean isDis = false;
                    if (robotsExsits) {
                        for (Pair<String, String> pair : rules) {
                            isDis = checkUrl(url, pair);
                            if (isDis) {
                                break;
                            }
                        }
                    }

                    if (isDis && disFlag.equals(Config.NO)) {
                        Put put = new Put(Bytes.toBytes(rowKey));
                        put.add(Bytes.toBytes(Config.CF_DOCS), Bytes.toBytes("disabled"), Bytes.toBytes(Config.YES));
                        context.write(null, put);
                    } else if (!isDis && disFlag.equals(Config.YES)) {
                        Delete del = new Delete(Bytes.toBytes(rowKey));
                        del.deleteColumn(Bytes.toBytes(Config.CF_DOCS), Bytes.toBytes("disabled"));
                        context.write(null, del);
                    }
                }
                rules.clear();
                robotsExsits = false;
            }
        }
    }

    public static class MyPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String host = key.toString().split(Config.DELIMITER)[0];
            return Math.abs(host.hashCode()) % numPartitions;
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("ROBOTS_HBASE");
        job.setJarByClass(TableJob.class);

        List<Scan> scans = new ArrayList<>();
        Scan scan1 = new Scan();
        scan1.addColumn(Bytes.toBytes(Config.CF_DOCS), Bytes.toBytes("url"));
        scan1.addColumn(Bytes.toBytes(Config.CF_DOCS), Bytes.toBytes("disabled"));
        scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(args[0]));
        scans.add(scan1);

        Scan scan2 = new Scan();
        scan2.addColumn(Bytes.toBytes(Config.CF_INFO), Bytes.toBytes("site"));
        scan2.addColumn(Bytes.toBytes(Config.CF_INFO), Bytes.toBytes("robots"));
        scan2.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(args[1]));
        scans.add(scan2);

        TableMapReduceUtil.initTableMapperJob(scans, MyMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(Config.WEBPAGES, MyReducer.class, job);

        job.setPartitionerClass(MyPartitioner.class);

        job.setNumReduceTasks(Config.COUNT_REDUCERS);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(HBaseConfiguration.create(), new TableJob(), args));
    }
}
