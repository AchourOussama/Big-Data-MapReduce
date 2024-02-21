package tn.insat.tp1.countCreditCards;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CreditCardsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text creditCard = new Text();

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        // splitting each row into array of substrings based on whitespaces
        String[] columns = value.toString().split("\\s+");
        // discarding the option "cash" from the keys
        if (columns.length > 0) {
            String paymentMethod = columns[columns.length - 1];
            if ( ! columns[columns.length - 1].equalsIgnoreCase("Cash")){
                System.out.println(paymentMethod);
                creditCard.set( columns[columns.length - 1]);
                context.write(creditCard, one);
            }

        }
    }
}
