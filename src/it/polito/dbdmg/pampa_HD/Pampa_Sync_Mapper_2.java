package it.polito.dbdmg.pampa_HD;

/**
 * @author Fabio Pulvirenti
 * @version 0.0.1
 */



import it.polito.dbdmg.pampa_HD.Pampa_Sync_Reducer.Heartbeat;
import it.polito.dbdmg.pampa_HD.util.row_rid;
import it.polito.dbdmg.pampa_HD.util.tableC;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * 
 */
class Pampa_Sync_Mapper_2 extends Mapper<
                    LongWritable, 
                    Text,         
                    Text,         
                    Text> {
    public static int minsup;
    static Hashtable closed_mod = new Hashtable<String, String >();
    static Hashtable seen_mod = new Hashtable<String, String >();
    
    public static class Heartbeat extends Thread {
		private static final int sleepTime = 400; // time in seconds
		private static boolean beating=true;
		private TaskInputOutputContext context = null;
		
		private Heartbeat (TaskInputOutputContext context) {
			this.context=context;
		}
		
		@Override
		public void run() {
			while (beating) {
				try {
					Thread.sleep(sleepTime*1000);
				}
				catch (InterruptedException e) {}
				context.setStatus(Long.valueOf(System.currentTimeMillis()).toString());
			}
		}
		
		public static void stopbeating () {
			beating=false;
		}
		
		public static Heartbeat createHeartbeat (TaskInputOutputContext context) {
			Heartbeat heartbeat = new Heartbeat(context);
			Thread heartbeatThread = new Thread(heartbeat);
			heartbeatThread.setPriority(MAX_PRIORITY);
			heartbeatThread.setDaemon(true);
			heartbeatThread.start();
			return heartbeat;
			
		}
		
	}
    
    
    
			
	
   
    
    
    
	protected void setup(Context context) throws IOException, InterruptedException
	{
		Heartbeat.createHeartbeat(context);
    	// read minsup
		minsup = Integer.parseInt(context.getConfiguration().get("minsup"));
    	
	}
	
	protected void cleanup (Context context) throws IOException, InterruptedException {
		Heartbeat.stopbeating();
	}
	
    @Override
    protected void map(
            LongWritable key,   
            Text value,         
            Context context) throws IOException, InterruptedException {

            
    		
    		String row=value.toString();
    		if (!row.startsWith("*")) {return;}
    		int dataset_size=0;
    		
    		if (!row.startsWith("*")) {
    		
    		String [] row1 = row.split("\\*\t");
    		String oldkey= row1[0]+"*";  //oldkey
    		String oldvalue= row1[1];  // and  oldvalue ready to be sent
            //analyse the header
    		String [] head2 = row1[0].split("\\|\\|");
    		String projection = head2[0].replaceAll("\\[","").replaceAll("\\]","");  // projection
            int deleted=Integer.parseInt(head2[1].trim());
            String [] projectionS= projection.split(",");
            List<Integer> projectionI= new ArrayList();
            for (String s: projectionS) projectionI.add(Integer.parseInt(s.trim()));
            String itemset = "";
            List<String> itemsetS1 = new ArrayList();
            List<String> itemsetS = new ArrayList();
            String [] tables = row1[1].split("\\|\\|");
            String itemset_complete="";
            String rowR=""; //not used anymore
            int nusm_rowR=0; //not used anymore
            tableC tab= new tableC();
            tab.modify_projection_and_list(projectionI);
            tab.add_deleted(deleted);
            int projection_length= tab.get_projection_size(); //not used anymore
            //System.out.println("\n proiezione della tab:"+tab.dammi_proiezione_stringa_spazi());
            for (int f=0;f<tables.length;f++) { //get the itemset! useful to understand if i have to stop
            	row_rid rowtemp;										// create the table
            	String [] row2= tables[f].split(",");
            	itemsetS1.add(row2[0]);
            	Collections.sort(itemsetS1);
            	}
            itemset_complete="";
            for (String s:itemsetS1) {itemset_complete=itemset_complete+" "+s;}           
            context.write(new Text(itemset_complete),new Text(projection+"||||"+oldkey+"|*|"+oldvalue));        
            if (closed_mod.containsKey(itemset_complete)) {}
            else if (tables.length==1) {}
            else {}
    		 return;}
    		else {
    			String[] parts1 = row.split("\t");
    			String oldkey=parts1[0];
    			String oldvalue=parts1[1];
    			String[] parts2= parts1[1].split("--");
    			String itemset_complete=parts1[0].replaceAll("\\*\\*\\*","");
    			String projection=parts2[0];
    			//System.out.println("\n sending: "+itemset_completo+" and value: "+proiezione+"||||"+oldkey+"|*|"+oldvalue);
                context.write(new Text(itemset_complete),new Text(projection+"||||"+oldkey+"|*|"+oldvalue));    			
    		}
    }
          
			
			
    }
