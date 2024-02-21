package tn.insat.tp1.countCreditCards;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

public class CreditCardsCount {
    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(CreditCardsCount.class);
        job.setJobName("CreditCardPurchases");

        job.setMapperClass(CreditCardsMapper.class);
        job.setReducerClass(CreditCardsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Input and Output paths
        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new org.apache.hadoop.fs.Path(args[0]));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
