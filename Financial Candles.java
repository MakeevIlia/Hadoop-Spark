import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;

import java.io.*;
import java.util.HashMap;

public class mm {

    public static boolean empty( final String s ) {
        return s == null || s.trim().isEmpty();
    }

    public static class Map_A extends Mapper <Object, Text, Text, Text>{
		private Text strkey = new Text();
        private Text strvalue = new Text();
		
        public void map (Object key, Text value,
                         Context context)
                throws IOException, InterruptedException 
		{
            Configuration conf = context.getConfiguration();
			
            int groups = Integer.parseInt(conf.get("mm.groups","1"));
            int n = Integer.parseInt(conf.get("n"));
			
            String[] matr_el = value.toString().split("[\t]");
			
            int Rlength = n / groups;
            int groupNum = Integer.parseInt(matr_el[1]) / Rlength;
			
			strkey.set(Integer.toString(groupNum) + Integer.toString(j));
			strvalue.set(Integer.toString(matr_el[3]));
			
            for(int j = 0; j < groups; ++j) 
			{
                context.write(new Text(strkey, strvalue);
            }
        }
    }

    public static class Map_B extends Mapper <Object, Text, Text, Text>{
		private Text strkey = new Text();
        private Text strvalue = new Text();
		
        public void map (Object key, Text value,
                         Context context)
                throws IOException, InterruptedException 
		{
            Configuration conf = context.getConfiguration();
			
            int groups = Integer.parseInt(conf.get("mm.groups","1"));
            int n = Integer.parseInt(conf.get("n"));
			
            String[] matr_el = value.toString().split("[\t]");
			
            int Clength = n / groups;
            int groupNum = Integer.parseInt(matr_el[1]) / Clength;
			
			strkey.set(Integer.toString(groupNum) + Integer.toString(i));
			strvalue.set(Integer.toString(matr_el[3]));
			
            for(int i = 0; i < groups; ++i) 
			{
                context.write(new Text(strkey, strvalue);
            }
        }
    }

    public static class Reduce extends Reducer <Text, Text, Text, Text>{

        public void reduce(Text key, Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int groups = Integer.parseInt(conf.get("mm.groups","1"));
            String ff = conf.get("mm.float-format", "%.3f");
            String[] tags = conf.get("mm.tags", "ABC").split("(?!^)");
            String Atag = tags[0];
            String Btag = tags[1];
            String Ctag = tags[2];
            int n = Integer.parseInt(conf.get("n"));
            int m = Integer.parseInt(conf.get("m"));
            int l = (n / groups);
			float[][] MatrA = new int[l][m];
			float[][] MatrB = new int[l][m];
            int r = Integer.parseInt(key.toString().split("(?!^)")[0]);
            int c = Integer.parseInt(key.toString().split("(?!^)")[1]);
            for (Text val : values) 
			{
                String[] parts = val.toString().split("[\t]");
                if (parts[0].equals(Atag)) 
				{
					MatrA[Integer.parseInt(parts[1])][Integer.parseInt(parts[2])] = Float.parseFloat(parts[3])
                } else if (parts[0].equals(Btag)) 
				{
                    MatrB[Integer.parseInt(parts[1])][Integer.parseInt(parts[2])] = Float.parseFloat(parts[3])
                }
            }
            float A, B, result;
            for (int i = 0; i < l; ++i) 
			{
                for (int j = 0; j < l; ++j) 
				{
                    result = 0.0f;
                    for (int k = 0; k < m; ++k) 
					{
                        result += MatrA[i][k] * MatrA[k][j];
                    }
                    if (result != 0.0f) 
					{
                        context.write(new Text(Ctag), new Text(Integer.toString(i+r*l) +"\t"+ Integer.toString(j+c*l) +"\t"+ String.format(ff, result)));
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        conf.set("mapred.textoutputformat.separator", "\t");

        FileSystem fs = FileSystem.get(conf);

        Path A_path= new Path(otherArgs[0]);
        Path B_path = new Path(otherArgs[1]);
        Path C_path = new Path(otherArgs[2]);
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(A_path.suffix("/size"))));
        String line = br.readLine();
        String n = line.split("[\t]")[0];
        String m = line.split("[\t]")[1];
        conf.set("n", n);
        conf.set("m", m);
        FSDataOutputStream outputStream=fs.create(C_path.suffix("/size"));
        
        outputStream.writeBytes(n + "\t" + n);
        outputStream.close();
        
        Job job = Job.getInstance(conf, "mm");
        job.setNumReduceTasks(Integer.parseInt(conf.get("mapred.reduce.tasks", "1")));
        job.setJarByClass(mm.class);
        
        MultipleInputs.addInputPath(job, A_path.suffix("/data"), TextInputFormat.class, Map_A.class);
        MultipleInputs.addInputPath(job, B_path.suffix("/data"), TextInputFormat.class, Map_B.class);
        job.setReducerClass(Reduce.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, C_path.suffix("/data"));
        System.exit(job.waitForCompletion(true) ? 0 : 2);
    }
}