
import java.io.DataInput;  
import java.io.DataOutput;  
import java.io.IOException;  
import java.net.URI;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.io.WritableComparable;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;  
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;  
   
public class MultiSort {
    private static final String INPUT_PATH = "hdfs://h71:9000/in";  
    private static final String OUT_PATH = "hdfs://h71:9000/out";  
      
    public static void main(String[] args) throws Exception {  
        Configuration conf = new Configuration();  
        FileSystem fileSystem = FileSystem.get(new URI(INPUT_PATH), conf);  
        if(fileSystem.exists(new Path(OUT_PATH))){  
            fileSystem.delete(new Path(OUT_PATH),true);  
        }  
        Job job = new Job(conf,MultiSort.class.getSimpleName());  
        FileInputFormat.setInputPaths(job, INPUT_PATH);  
        job.setJarByClass(MultiSort.class);
//�������б���ӣ���Ȼ�ᱨ����Caused by: java.lang.ClassNotFoundException: Class MultiSort$MyMapper not found
        
        //ָ���ĸ���������ʽ�������ļ�  
        job.setInputFormatClass(TextInputFormat.class);  
        //ָ���Զ����Mapper��  
        job.setMapperClass(MyMapper.class);  
        //ָ�����<k2,v2>������  
        job.setMapOutputKeyClass(newK2.class);  
        job.setMapOutputValueClass(LongWritable.class);  
          
        //ָ��������  
        job.setPartitionerClass(HashPartitioner.class);  
        job.setNumReduceTasks(1);  
        
        //ָ���Զ����reduce��  
        job.setReducerClass(MyReducer.class);  
        //ָ�����<k3,v3>������  
        job.setOutputKeyClass(LongWritable.class);  
        job.setOutputValueClass(LongWritable.class);  
          
        FileOutputFormat.setOutputPath(job, new Path(OUT_PATH));  
        //�趨����ļ��ĸ�ʽ����  
        job.setOutputFormatClass(TextOutputFormat.class);  
          
        //�Ѵ����ύ��JobTrackerִ��  
        job.waitForCompletion(true);  
    }  
      
    static class MyMapper extends Mapper<LongWritable,Text, newK2,LongWritable>{  
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splied = value.toString().split(" ");
            newK2 k2 = new newK2(Long.parseLong(splied[0]),Long.parseLong(splied[1]));
            final LongWritable v2 = new LongWritable(Long.parseLong(splied[1]));
            context.write(k2, v2);
        }
    }
      
    static class MyReducer extends Reducer<newK2, LongWritable, LongWritable, LongWritable>{  
        @Override  
        protected void reduce(MultiSort.newK2 key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {  
            context.write(new LongWritable(key.first), new LongWritable(key.second));  
        }  
    }  
      
    static class newK2 implements WritableComparable<newK2>{  
        Long first;  
        Long second;  
        public newK2(long first, long second) {  
            this.first = first;  
            this.second = second;  
        }  
  
        public newK2() {  
        }  //�������������ɾ�������򱨴���Caused by: java.lang.RuntimeException: java.lang.NoSuchMethodException: MultiSort$newK2.<init>()
  
        @Override  
        public void readFields(DataInput input) throws IOException {  
            this.first = input.readLong();  
            this.second = input.readLong();  
        }  
  
        @Override  
        public void write(DataOutput out) throws IOException {  
            out.writeLong(first);  
            out.writeLong(second);  
        }   
        //����һ�в�ͬʱ�����򣻵���һ����ͬʱ���ڶ������� 
           
        @Override  
        public int compareTo(newK2 o) {  
            long temp = this.first -o.first;  
            if(temp!=0){  
                return (int)temp;  
            }  
            return (int)(this.second -o.second);  
        }  
  
        @Override  
        public int hashCode() {  
            return this.first.hashCode()+this.second.hashCode();  
        }  
  
        @Override  
        public boolean equals(Object obj) {  
            if(!(obj instanceof newK2)){  
                return false;  
            }  
            newK2 k2 = (newK2)obj;  
            return(this.first == k2.first)&&(this.second == k2.second);  
        }  
    }  
}
��������������������������������
��Ȩ����������ΪCSDN������Сǿǩ����ơ���ԭ�����£���ѭCC 4.0 BY-SA��ȨЭ�飬ת���븽��ԭ�ĳ������Ӽ���������
ԭ�����ӣ�https://blog.csdn.net/m0_37739193/article/details/76090816