package it.polito.dbdmg.pampa_HD;

/**
 * @author Fabio Pulvirenti
 * @version 0.9
 */

/**Carpenter A Mapper - This job takes the vertical dataset as input and creates the first level tables for the reducer
*/




import it.polito.dbdmg.pampa_HD.util.comparator_A;
import it.polito.dbdmg.pampa_HD.util.row_A;
import it.polito.dbdmg.pampa_HD.util.tableA;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;


/**
 * Carpenter A Mapper - This job takes the vertical dataset as input and creates the first level tables for the reducer
 */
class Pampa_A_Mapper extends Mapper<LongWritable,Text,Text,Text> 
{
    public static int minsup;
  
	protected void setup(Context context) throws IOException, InterruptedException
	{
    	minsup = Integer.parseInt(context.getConfiguration().get("minsup"));
    }
	
    @Override
    protected void map(
            LongWritable key, 
            Text value,        
            Context context) throws IOException, InterruptedException 
            {                		
            String[] words = value.toString().split(",");
            String[] transaction_strings=words[1].split(" ");
            // first pruning: lower minsup rows are discarded
            if (transaction_strings.length>=minsup){
            	List<Integer> integer_list = new ArrayList<Integer>();
            	//add to an Integer list: each list is a tidlist
                for(String word : transaction_strings) 
                {
                	integer_list.add(Integer.parseInt(word));
                }           
                for (int i=0; i<integer_list.size(); i++) 
                {
                	String head;
                	String transtemp="";
                	head= integer_list.get(i).toString();
                	int b=1;
                	for (int a=i+1; a<(integer_list.size()); a++)
                	{	
                		b++;
                		transtemp=transtemp+integer_list.get(a).toString()+" ";
                	}
                	if (b>=minsup){//Partial list can not be over the minsup
                		context.write(new Text(head),new Text(words[0]+","+transtemp));
                	}
                	
                }
            }
         }
}