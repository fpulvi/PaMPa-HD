package it.polito.dbdmg.pampa_HD;

/**
 * @author Fabio Pulvirenti
 * @email fabio.pulvirenti@polito.it
 * @version 0.0.1
 */






import it.polito.dbdmg.pampa_HD.Pampa_Sync_Mapper_1.Heartbeat;
import it.polito.dbdmg.pampa_HD.util.row_rid;
import it.polito.dbdmg.pampa_HD.util.tableC;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * WordCount Reducer
 */
class Pampa_Sync_Reducer extends Reducer<
                Text,           
               Text,   
                Text,         
               Text> {
    
	static int found_tab=0;
	static int minsup;
	static Hashtable closed = new Hashtable<String, Integer>();
	static Hashtable closed_mod = new Hashtable<String, String >();
	static Hashtable seen_mod = new Hashtable<String, String >();
	static int dataset_size=0;
	static int key_count=0;
	static int total_closed=0;
	static int drop_count=0;
	static int kept_tables;
	static List<String> seen = new ArrayList<String>(); //not used
	
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
	
	private static void print_table(tableC tab, Context context ) throws IOException, InterruptedException {
		String tabstring;
		int size = tab.get_list().size();
		int deleted =tab.get_deleted();
		String sizes= Integer.toString(size);
		tabstring=tab.get_projection().toString()+"||"+sizes+"||"+deleted+"*";
		//context.write(new Text("*"+tab.dammi_proiezione().toString()), new Text(sizes));
		
		// header separator *
		//within header, separator projection with table legnth ||
		//within table, row separator ||
		//within row, separator item and transactions ,
		String tabstring2="";
		//System.out.println("\n this is transposed table"+tabtemp.dammi_proiezione()+" containing "+tabtemp.mostraitemsetString());
		for (row_rid r: tab.get_list()) {
			tabstring2=tabstring2+r.as_string()+" ||";
			
			
			//System.out.println("\n"+r.mostraitem()+" "+ r.mostralista_trans());				
			}
		context.write(new Text(tabstring), new Text(tabstring2));
	}
	
	

	public static int comparator_f (String key1, String key2)
	{int comparison=0;
	String[] k1= key1.split(",");
	String[] k2= key2.split(",");
	List<Integer> projection1 = new ArrayList<Integer>();
	List<Integer> projection2 = new ArrayList<Integer>();
	
	for (String s: k1) projection1.add(Integer.parseInt(s.trim()));
	for (String s: k2) projection2.add(Integer.parseInt(s.trim()));
	if (projection1.size()==projection2.size()) {
		for (int b=0; (b<projection1.size())&(b<projection2.size());b++) {
			comparison=Integer.compare(projection1.get(b),projection2.get(b));
		if (comparison!=0) return comparison;
		}
		return comparison;
		}
	else {
	
		for (int b=0; (b<projection1.size())&(b<projection2.size());b++) {
			comparison=Integer.compare(projection1.get(b),projection2.get(b));
			if (comparison!=0) return comparison;
			}
		if (projection2.size()>projection1.size()) { 
			return -1;}
		else if (projection1.size()>projection2.size()){   
			return 1;}
		else { 
			return 0;}
		}
	}
	

		
	
	

	protected void setup(Context context) throws IOException, InterruptedException
	{
    	// Leggo il minsup
		Heartbeat.createHeartbeat(context);
		minsup = Integer.parseInt(context.getConfiguration().get("minsup"));
		
    	
	}
	
	protected void cleanup (Context context) throws IOException, InterruptedException {
		Heartbeat.stopbeating();
		System.out.println("\n on: "+key_count+" keys and a total number of closed:"+total_closed+" has been dropped "+drop_count+" redundant tables");
		context.getCounter("total_closed_counter","total_closed_counter").increment(total_closed);
		context.getCounter("drop_count_counter","drop_count_counter").increment(drop_count);
		context.getCounter("kept_tables","kept_tables").increment(kept_tables);
		if (found_tab>0) {
		context.getCounter("found","found").increment(5);
	
		}  //for the driver
	}
	
	
    @Override
    protected void reduce(
        Text key, 
        Iterable<Text> rigaTT, 
        Context context) throws IOException, InterruptedException {
    	tableC tab=new tableC(); 
    	key_count++;
    	
    	int dropped_iteration=0;
        // TODO: implement here Reduce code
        //int occurrances = 0;
       // for (IntWritable rigatemp : righe) {
       //     occurrances += rigatemp.get();
       // }
       // context.write(key, new IntWritable(occurrances));
    	
    	//System.out.println("\n just arrived "+key.toString()+" | "+rigaTT.toString());
    	//table building
    	if (key.toString().startsWith("\\*\\*\\*")) {//useless
    		
    		//System.out.println("\n row 188");
    		String itemset2=key.toString();
    		String[] itemset=itemset2.replaceAll("\\*\\*\\*","").split("\\|\\|");
			int supmax=0;
			for (Text m: rigaTT)
	    	{   dropped_iteration++;
	    		total_closed++;
				String[] r=m.toString().split("\\|\\|");
    			int num_item=r.length;
    			//System.out.println("\njust arrived "+key.toString()+" | "+m.toString()+" with num_item= "+num_item);
				for (int y=0;y<num_item;y++)
				{
						//System.out.println("\n just arrived "+key.toString()+" | "+r.toString());
	    	    String[] parti2 = r[y].split("--");	    		
				if (closed_mod.containsKey(itemset[y])) { // if it is here, have to do checks
					String[] parts = ((String) closed_mod.get(itemset[y])).split("--");
					String projection_old= parts[0];
					int comparison = comparator_f(projection_old,parti2[0]);
					if (comparison<=0) {
						//System.out.println("\n the comparable told it already exists: this itemsset: "+itemset+" found here:" +parti2[0]+" was found here: "+proiezione_vecchia);
						return;
						}
					else {
						closed_mod.put(itemset[y],parti2[0]+"--"+parti2[1]);
						//System.out.println("\na arrived directly and just added to the closed "+itemset+ " with sup "+parti2[1]); 
						}
					}
				closed_mod.put(itemset[y],parti2[0]+"--"+parti2[1]);
					//System.out.println("\n arrived directly and just added to the closed "+itemset[y]+ " with sup "+parti2[1]);   
	    	    
	    	}
	    		}
			
    		}
    	else {
	    	
    		String best_projection="";
    		String best_oldkey="";
    		String best_oldvalue="";
	    	
	    	for (Text m: rigaTT)
		    	{	dropped_iteration++;
	    			total_closed++;
					//numerorighedataset++;  not needed anymore
	    			//System.out.println("\n just arrived this "+key.toString()+"\t"+m.toString());
	    			String[] r=m.toString().split("\\|\\|\\|\\|");
	    			String cand_projection=r[0];
	    			if (best_projection.equals("")) {
	    				
	    				best_projection=cand_projection;
	    				String[] parts2= r[1].split("\\|\\*\\|");
	    				
	    				best_oldkey=parts2[0];
	    				best_oldvalue=parts2[1]+"|*|"+key.toString();
	    				
	    			}
	    			else {
	    				//System.out.println("comparing: "+best_proiezione+" with "+cand_proiezione);
	    				int comparison = comparator_f(best_projection,cand_projection);
	    				
	    				if (comparison>0) { //the old one has to be dropped
	    				/*	if (!best_oldkey.startsWith("\\*\\*\\*")) {tables_not_expanded++;  
	    					System.out.println("\n just dropped "+best_oldkey+"\t"+best_oldvalue); } */
	    					best_projection=cand_projection;
		    				String[] parts2= r[1].split("\\|\\*\\|");
		    				best_oldkey=parts2[0];
		    				best_oldvalue=parts2[1]+"|*|"+key.toString();
		    						
		    				
	    				}
	    			}
		    	}
	    	//System.out.println("\ns ending "+best_oldkey+" and "+best_oldvalue);
	    	if (!best_oldkey.contains("***")) {
	    		found_tab=1;
	    		//System.out.println("\n"+best_oldkey+" no stars");
	    		kept_tables++;
	    		//System.out.println("\n just sending "+best_oldkey+" which is a table and not a closed");
	    	}
	    	context.write(new Text(best_oldkey), new Text(best_oldvalue));
	    
    	}		
    	drop_count=drop_count+(dropped_iteration-1);	

    	
    	
    }
}