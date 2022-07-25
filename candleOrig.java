import java.io.IOException;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class candle {

    public static String securities = ".*";
    public static int date_from = 19000101;
    public static int date_to = 20200101;
    public static int time_from = 1000;
    public static int time_to = 1800;

    public static class IDs{
        public static int symbol_id = 0;
        //public static int system_id = 1;
        public static int moment_id = 2;
        public static int id_deal_id = 3;
        public static int price_deal_id = 4;
        //public static int volume_id = 5;
        //public static int open_pos_id = 6;
        //public static int direction_id = 7;
    }

    public static boolean empty( final String s ) {
        // Null-safe, short-circuit evaluation.
        return s == null || s.trim().isEmpty();
    }

    public static class MyMap extends Mapper <Object, Text, Text, Text>{
        private Text deal = new Text();
        private Text line = new Text();
        public void map (Object key, Text value,
                         Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int width = Integer.parseInt(conf.get("candle.width"));


            if( !empty(value.toString())) {

                String[] parts = value.toString().split("[,]");

                if (value.toString().startsWith("#")) {
                    int i = 0;
                    for (String part : parts) {
                        if (part.contains("SYMBOL")) IDs.symbol_id = i;
                        if (part.contains("MOMENT")) IDs.moment_id = i;
                        if (part.contains("ID_DEAL")) IDs.id_deal_id = i;
                        if (part.contains("PRICE_DEAL")) IDs.price_deal_id = i;
                        i++;
                    }
                } else {
                    String date = parts[IDs.moment_id].substring(0, 8);
                    String time = parts[IDs.moment_id].substring(8, 17);
                    if (Pattern.matches(securities, parts[IDs.symbol_id])) {
                        if (date_from <= Integer.parseInt(date) && date_to >= Integer.parseInt(date)) {
                            if (time_from * 100000 <= Integer.parseInt(time) && time_to * 100000 >= Integer.parseInt(time)) {
                                int num = ((Integer.parseInt(time.substring(0, 2)) - time_from / 100) * 3600000 +
                                        (Integer.parseInt(time.substring(2, 4)) - time_from % 100) * 60000 +
                                        Integer.parseInt(time.substring(4, 9))) / width;

                                line.set(parts[IDs.moment_id] + "," + parts[IDs.id_deal_id] + "," + parts[IDs.price_deal_id]);
                                deal.set(parts[IDs.symbol_id] + "," + date + "," + Integer.toString(num));

                                context.write(deal, line);
                            }
                        }
                    }
                }
            }
        }
    }


    public static class MyReduce extends Reducer <Text, Text, Text, Text>{
        private Text line = new Text();
        private int moment;
        private MultipleOutputs<Text, Text> out;

        public void setup(Context context) {
            out = new MultipleOutputs<Text, Text>(context);
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int width = Integer.parseInt(conf.get("candle.width"));

            float open = 0;
            float high = 0;
            float low = Float.MAX_VALUE;
            float close = 0;

            int time_open = time_to*100000;
            int id_close = 0;
            int id_open = Integer.MAX_VALUE;
            int time_close = time_from*100000;

            String[] parts_k = key.toString().split("[,]");
            int num = Integer.parseInt(parts_k[2]);
            int moment_start = (time_from/100)*3600000 + (time_from%100)*60000 + num*width;

            for (Text val : values) {
                String[] parts_v = val.toString().split("[,]");

                int time = Integer.parseInt(parts_v[0].substring(8,17));
                int id = Integer.parseInt(parts_v[1]);
                float price = Float.parseFloat(parts_v[2]);

                if (time_open > time){
                    time_open = time;
                    id_open = id;
                    open = price;
                }
                else if (time_open == time){
                    if (id_open > id){
                        id_open = id;
                        open = price;
                    }
                }
                if (time_close < time){
                    time_close = time;
                    id_close = id;
                    close = price;
                }
                else if (time_close == time){
                    if (id_close < id){
                        id_close = id;
                        close = price;
                    }
                }
                if (high < price){
                    high = price;
                }
                if (low > price){
                    low = price;
                }
            }

            line.set(new Text (parts_k[1] + String.format("%02d",((moment_start/1000)/60)/60) + String.format("%02d",((moment_start/1000)/60)%60) +
                    String.format("%02d",(moment_start/1000)%60) + String.format("%03d",(moment_start%1000))) +
                    "," + String.format(Locale.US,"%.1f",open) + "," + String.format(Locale.US,"%.1f",high) +
                    "," + String.format(Locale.US,"%.1f",low) + "," + String.format(Locale.US,"%.1f",close));
            key.set(new Text(parts_k[0]));

            out.write(key, line, key.toString());
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            out.close();
        }
    }


    public static void  main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "candle");
        job.setNumReduceTasks(Integer.parseInt(conf.get("candle.num.reducers")));

        job.setJarByClass(candle.class);
        job.setMapperClass(MyMap.class);
        //job.setCombinerClass(MyReduce.class);
        job.setReducerClass(MyReduce.class);

        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}