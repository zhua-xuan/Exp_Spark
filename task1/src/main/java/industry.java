import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class industry {
    public static class industryMapper extends Mapper<Object, Text, Text, IntWritable>{ //map函数
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            IntWritable one = new IntWritable(1);//处理表头
            String word = value.toString();
            String[] line = value.toString().split(",");  //用逗号分割
            context.write(new Text(line[10]), one);
            }
        }
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {//实现reduce()函数
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {   //遍历迭代values，得到同一key的所有value
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    private static class IntWritableDecreasingComparator extends IntWritable.Comparator { //降序排列
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        Job job = Job.getInstance(conf, "industry");
        job.setJarByClass(industry.class);
        job.setMapperClass(industryMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; i++) {
            otherArgs.add(remainingArgs[i]);
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        Path tempDir = new Path("tmp" );
        FileOutputFormat.setOutputPath(job, tempDir);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        Job sortJob = Job.getInstance(conf, "sortIndustry");
        sortJob.setJarByClass(industry.class);
        FileInputFormat.addInputPath(sortJob, tempDir);
        sortJob.setInputFormatClass(SequenceFileInputFormat.class);
        sortJob.setMapperClass(InverseMapper.class);
        FileOutputFormat.setOutputPath(sortJob, new Path(otherArgs.get(1)));
        sortJob.setOutputKeyClass(IntWritable.class);
        sortJob.setOutputValueClass(Text.class);
        sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class); //降序
        FileSystem.get(conf).deleteOnExit(tempDir);
        System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
    }
}
