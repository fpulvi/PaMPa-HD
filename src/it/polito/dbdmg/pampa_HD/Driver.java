package it.polito.dbdmg.pampa_HD;

/**
 * @author Fabio Pulvirenti
 * @version 0.1.0
 */

import java.util.ArrayList;
import java.util.List;

import it.polito.dbdmg.pampa_HD.util.comparator_A;
import it.polito.dbdmg.pampa_HD.util.row_A;
import it.polito.dbdmg.pampa_HD.util.tableA;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;





/**
 * Carpenter - Step A  - this job reads a VERTICAL dataset and computes the first K tables in a depth first fashion
 * Mapper now creates the table and reducer starts to develop the tree
 */
public class Driver extends Configured implements Tool {

    private int numberOfReducers;
    private int minsup;
    private int max_tables;
    private int steps;
    private Path inputPath;
    private Path Input;
    private Path Input2;
    private Path FinalOutputDir;
    private int TempOutput;
    private Path Output;
    private int TempInput;
    private int numpath;
    private long tab_found;
    List<Double> trend_of_pruning = new ArrayList<Double>();
    List<Double> trend_of_pruning_tables = new ArrayList<Double>();

    private int br_to_deep;
    public static long sent_tables_job_a=0;
    @Override
    public int run(String[] args) throws Exception {
        TempOutput=0;
        Output = new Path(this.FinalOutputDir+"/"+TempOutput);
        run_A();
        numpath=1;
        TempInput=TempOutput;
        TempOutput++;
        Input = new Path(this.FinalOutputDir+"/"+TempInput);
        Input2 = new Path(this.FinalOutputDir+"/"+(TempInput-1));
        Output = new Path(this.FinalOutputDir+"/"+TempOutput);
        tab_found=0;
        run_rid();
        numpath=2;
        while(1>0) {
            br_to_deep++;
            max_tables=max_tables*steps;
            if (max_tables<0) max_tables=2000000000;
            numpath=2;
            TempInput=TempOutput;
            TempOutput++;
            Output = new Path(this.FinalOutputDir+"/"+TempOutput);
            Input = new Path(this.FinalOutputDir+"/"+TempInput);
            System.out.println("\n Tables to expand found, starting another iteration\n");
            run_B();
            if (tab_found==0) break;
        }
        System.out.println("\n No tables found, merging the results in an unique file\n");
        numpath=1;
        TempInput=TempOutput;
        TempOutput++;
        Input = new Path(this.FinalOutputDir+"/"+TempInput);
        Input2 = new Path(this.FinalOutputDir+"/"+(TempInput-1));
        Output = new Path(this.FinalOutputDir+"/FI");
        numberOfReducers=1;
        run_rid();
        System.out.println("this is the total trend of pruning "+trend_of_pruning.toString());
        System.out.println("this is the trend of pruning for the tables "+trend_of_pruning_tables.toString());
        return 1;

    }

    public int run_A() throws Exception {
        Configuration conf = this.getConf();

        conf.set("minsup",Integer.toString(this.minsup));
        conf.set("max_tables",Integer.toString(this.max_tables));

        Job job = new Job(conf);

        job.setJobName("PAMPA_A_minsup:"+Integer.toString(this.minsup)+"_input:"+this.inputPath);

        FileInputFormat.addInputPath(job, this.inputPath);

        FileOutputFormat.setOutputPath(job, Output);

        job.setJarByClass(Driver.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(Pampa_A_Mapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Pampa_A_Reducer.class);

        //this is a comparator self written to force an arrival order to the reducer
        job.setSortComparatorClass(comparator_A.class);

        job.setNumReduceTasks(this.numberOfReducers);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        if (job.waitForCompletion(true)==true)
        {//conf = job.getConfiguration();
            sent_tables_job_a = job.getCounters().findCounter("sent_tables","sent_tables").getValue();

            return 0;}

        else {
            conf = job.getConfiguration();
            return 1;}
        // return job.waitForCompletion(true) ? 0 : 1;
    }


    public int run_rid () throws Exception {
        Configuration conf = this.getConf();

        conf.set("minsup",Integer.toString(this.minsup));

        Job job = new Job(conf);

        job.setJobName("Sync/Delete_redundancy_job");

        MultipleInputs.addInputPath(job, Input, TextInputFormat.class, Pampa_Sync_Mapper_1.class);

        if (numpath>1) {
            MultipleInputs.addInputPath(job, Input2, TextInputFormat.class, Pampa_Sync_Mapper_2.class);}

        FileOutputFormat.setOutputPath(job, Output);

        job.setJarByClass(Driver.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Pampa_Sync_Reducer.class);

        job.setNumReduceTasks(this.numberOfReducers);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        tab_found=0;
        long local_drop_count=0;
        long local_total_closed=0;
        long local_kept_tables=0;
        if (job.waitForCompletion(true)==true)
        {
            tab_found = job.getCounters().findCounter("found","found").getValue();
            local_drop_count = job.getCounters().findCounter("drop_count_counter","drop_count_counter").getValue();
            local_total_closed = job.getCounters().findCounter("total_closed_counter","total_closed_counter").getValue();
            local_kept_tables=  job.getCounters().findCounter("kept_tables","kept_tables").getValue();
            System.out.println("\n on a total number of closed:"+local_total_closed+" has been dropped "+local_drop_count+" redundant values");
            System.out.println("\n on a total number of sent tables: "+sent_tables_job_a+" has been kept "+local_kept_tables+" redundant tables");
            double  ratio= (double) local_drop_count/(1+local_total_closed);
            trend_of_pruning.add( ratio);
            ratio= (double) (sent_tables_job_a-local_kept_tables)/(1+sent_tables_job_a);
            trend_of_pruning_tables.add (ratio);
            return 0;}

        else {
            conf = job.getConfiguration();
            return 1;}
    }


    public int run_B () throws Exception {
        Configuration conf = this.getConf();

        conf.set("minsup",Integer.toString(this.minsup));
        conf.set("br_to_deep", Integer.toString(this.br_to_deep));
        conf.set("max_tables",Integer.toString(this.max_tables));

        Job job = new Job(conf);

        job.setJobName("PAMPA_B_minsup:"+Integer.toString(this.minsup)+"__"+Integer.toString(this.br_to_deep));

        FileInputFormat.addInputPath(job, Input);

        FileOutputFormat.setOutputPath(job, Output);

        job.setJarByClass(Driver.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(Pampa_B_Mapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(Pampa_Sync_Reducer.class);
        job.setNumReduceTasks(this.numberOfReducers);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        long local_drop_count;
        long local_total_closed;
        long local_total_sent_tables;
        long local_kept_tables;
        tab_found=0;
        if (job.waitForCompletion(true)==true)
        {
            tab_found = job.getCounters().findCounter("found","found").getValue();
            local_drop_count = job.getCounters().findCounter("drop_count_counter","drop_count_counter").getValue();
            local_total_closed = job.getCounters().findCounter("total_closed_counter","total_closed_counter").getValue();
            local_total_sent_tables=  job.getCounters().findCounter("sent_tables","sent_tables").getValue();
            local_kept_tables=  job.getCounters().findCounter("kept_tables","kept_tables").getValue();
            System.out.println("\n on a total number of closed:"+local_total_closed+" has been dropped "+local_drop_count+" redundant values");
            System.out.println("\n on a total number of sent tables: "+local_total_sent_tables+" has been kept "+local_kept_tables+" redundant tables");
            double  ratio= (double) local_drop_count/(1+local_total_closed);
            trend_of_pruning.add( ratio);
            ratio= (double) (local_total_sent_tables-local_kept_tables)/(1+local_total_sent_tables);
            trend_of_pruning_tables.add (ratio);
            return 0;}

        else {
            conf = job.getConfiguration();
            return 1;}
    }

    public Driver (String[] args) {
        if (args.length != 6) {
            System.out.println("Usage: Pampa max_tables++ <num_reducers> <input_path> <output_path> <minsup> <max_tables_start> <steps>");
            System.exit(0);
        }
        this.numberOfReducers = Integer.parseInt(args[0]);
        this.minsup=Integer.parseInt(args[3]);
        this.max_tables=Integer.parseInt(args[4]);
        this.inputPath= new Path(args[1]);
        this.steps= Integer.parseInt(args[5]);
        this.FinalOutputDir = new Path(args[2]);
    }

    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Driver(args), args);
        System.exit(res);
    }
}
