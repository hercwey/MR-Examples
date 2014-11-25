package org.pungii.myzk.hadoop;

import hdfs.HdfsDAO;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * 销售
 * 
 * @author conan
 */
public class Sell {

    public static final String HDFS = "hdfs://192.168.1.107:9100";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static class SellMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String month = "2013-02";
        private Text k = new Text(month);
        private Text v = new Text();
        private int money = 0;

        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            System.out.println(values.toString());
            String[] tokens = DELIMITER.split(values.toString());
            if (tokens[3].startsWith(month)) {// 2月的数据
                money = Integer.parseInt(tokens[1]) * Integer.parseInt(tokens[2]);//单价*数量
                v.set(money+","+tokens[0]);
                context.write(k, v);
            }
        }
    }

    public static class SellReducer extends Reducer<Text, Text, Text, Text> {
        private IntWritable v = new IntWritable();
        private Integer max = new Integer(0);
        private String maxId = "";
        private int money = 0;

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            for (Text line : values) {
                // System.out.println(key.toString() + "\t" + line);
                String[] tokens = DELIMITER.split(line.toString());
                Integer sub= Integer.parseInt(tokens[0]);
                if(sub.compareTo(max)>0){
                    max=sub;
                    maxId = tokens[1];
                } 
                money += sub;
            }
            v.set(money);
            context.write(null, new Text(v+","+maxId));
            System.out.println("Output:" + key + "," + money);
        }

    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = config();
        String local_data = path.get("sell");
        String input = path.get("input");
        String output = path.get("output");

        // 初始化sell
        HdfsDAO hdfs = new HdfsDAO(HDFS, conf);
        hdfs.rmr(input);
        hdfs.mkdirs(input);
        hdfs.copyFile(local_data, input);

        Job job = new Job(conf);
        job.setJarByClass(Sell.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SellMapper.class);
        job.setReducerClass(SellReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

    public static JobConf config() {// Hadoop集群的远程配置信息
        JobConf conf = new JobConf(Purchase.class);
        conf.setJobName("purchase");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }
    
    public static Map<String,String> path(){
        Map<String, String> path = new HashMap<String, String>();
        path.put("sell", "logfile/biz/sell.csv");// 本地的数据文件
        path.put("input", HDFS + "/user/hdfs/biz/sell");// HDFS的目录
        path.put("output", HDFS + "/user/hdfs/biz/sell/output"); // 输出目录
        return path;
    }

    public static void main(String[] args) throws Exception {
        run(path());
    }

}
