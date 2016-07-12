package it.polito.dbdmg.pampa_HD;

/**
 *
USELESS, HERE IN THE NEXT DEVELOPMENTS, THE SYNC JOB WILL BE  INCLUDED
 */






import it.polito.dbdmg.pampa_HD.Pampa_B_Mapper.Heartbeat;
import it.polito.dbdmg.pampa_HD.util.comparator_B;
import it.polito.dbdmg.pampa_HD.util.row_B;
import it.polito.dbdmg.pampa_HD.util.tableB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.Reducer.Context;



/**
 * WordCount Reducer
 */
class Pampa_B_Reducer extends Reducer<
                Text,           // TODO: replace Object with input key type
               Text,    // TODO: replace Object with input value type
                Text,           // TODO: replace Object with output key type
               Text> {// TODO: replace Object with output value type
    
	
	static int minsup;
	static int max_tables;
	static int br_to_deep;
	static int iter=0;
	static Hashtable seen_mod = new Hashtable<String, String >();
	static int dataset_size=0;
	static int b=0;
	int c=0;
	static List<String> seen = new ArrayList<String>();
	
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
	
	private static void print_table(tableB tab, Context context ) throws IOException, InterruptedException {
		String tabstring;
		//int size = tab.dammi_lista().size();
		int deleted =tab.get_deleted();
		//String sizes= Integer.toString(size);
		tabstring=tab.get_projection().toString()+"||"+deleted+"*";

		String tabstring2="";
		for (row_B r: tab.get_list()) {
			tabstring2=tabstring2+r.as_string()+" ||";
			
			
			}
		context.write(new Text(tabstring), new Text(tabstring2));
	}
	
	

	public static int comparator_f (String key1, String key2)
	{int comparison=0;
	String[] k1= key1.split(",");
	String[] k2= key2.split(",");
	List<Integer> projection1 = new ArrayList<Integer>();
	List<Integer> projection2 = new ArrayList<Integer>();
	//System.out.println("\nsto per processare: "+key1.toString()+" e "+key2.toString());
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
	//System.out.print("\n sono dentroooo");
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
	
	private static void recursive (tableB tab, Context context) throws IOException, InterruptedException
	{ 	iter++;
		List<Integer> projection =new ArrayList<Integer>();
		for (int a: tab.get_projection()) projection.add(a);
		

		
		

		 if ((tab.get_list().size()==1) & (tab.max_length()>=minsup)) {
			 
			 	int deleted = tab.get_deleted();
				String itemset = tab.showitemsetString();
				int projection_size= tab.get_projection_size();
				int length_max_rows=tab.max_length();
	        	// ha solo una voce, non ha senso dividerla
	        	int num_row=tab.max_length();
	        	String itemset_complete=tab.showitemsetString();
	        	context.write(new Text("***"+itemset_complete), new Text(tab.get_projection_string()+"--"+Integer.toString(num_row)));
	        	
	        	 return; }
		
		List<Integer> elements_in_all = tab.elements_in_all(dataset_size);
		if (elements_in_all.size()>0) {
			tab.modify_only_list(elements_in_all);
		}
		int deleted = tab.get_deleted();
		String itemset = tab.showitemsetString();
		int projection_size= tab.get_projection_size();
		int length_max_rows=tab.max_length();

		if (projection_size>=minsup)	
		context.write(new Text("***"+itemset), new Text(tab.get_projection_string()+"--"+Integer.toString(projection_size)));

		
		

		List <Integer> found=new ArrayList();
		
		for (int i=1;i<=dataset_size;i++) {
			boolean found2=false;
			tableB tabtemp=new tableB();
			for (row_B r: tab.get_list()){
				if (r.find_first_and_del((Integer) i)) {

					row_B r2= (row_B) r.clone();

					if (r2.length()+projection_size+1<minsup) {
						continue;
					}
					found2=true;
					tabtemp.add_row(r2);
					
				}				
			}
			if (found2) {
				found.add(i);
			tabtemp.add_deleted(deleted);
			
			tabtemp.create_projection_modify_list(projection,i);

			
			String itemset_new=tabtemp.showitemsetString();
			String projection_new=tabtemp.get_projection_string_spaces();
			
			if (seen_mod.containsKey(itemset_new)) {
				String[] parts2 = ((String) seen_mod.get(itemset_new)).split("--");
				String proiezione_vecchia= parts2[0];
				int confronto = comparator_f(proiezione_vecchia,projection_new);
				if (confronto<=0) {
				continue;
				}
				else {seen_mod.put(itemset_new,projection_new+"--"+Integer.toString(projection_size+deleted));
            	}
			}
			else {seen_mod.put(itemset_new,projection_new+"--"+Integer.toString(projection_size+deleted));
				}
			if ((tabtemp.get_list().size()==1) & (tabtemp.max_length()>=minsup)){
				 
			 	int deleted2 = tabtemp.get_deleted();
				String itemset2 = tabtemp.showitemsetString();
				int projection_size2= tabtemp.get_projection_size();
				int length_max_row2=tab.max_length();
	        	// ha solo una voce, non ha senso dividerla
	        	int num_row_row2=tabtemp.max_length();
	        	String itemset_complete2=tabtemp.showitemsetString();
	        	continue; }
			

			int found3=0;
			int mb=1024*1024;
			Runtime runtime = Runtime.getRuntime();
			if (((runtime.freeMemory()) / mb)<50) seen_mod.clear();
			if ((((runtime.freeMemory()) / mb)>25)&&(iter<=max_tables)) {recursive(tabtemp,context);}
			else {print_table(tabtemp,context);
				}
			tabtemp=null;

		
			}
			
	}

		return;
			}
	

	protected void setup(Context context) throws IOException, InterruptedException
	{
    	// Leggo il minsup
		Heartbeat.createHeartbeat(context);
		minsup = Integer.parseInt(context.getConfiguration().get("minsup"));
		br_to_deep= Integer.parseInt(context.getConfiguration().get("br_to_deep"));
		max_tables = Integer.parseInt(context.getConfiguration().get("max_tables"));
		
    	
	}
	
	protected void cleanup (Context context) throws IOException, InterruptedException {
		Heartbeat.stopbeating();
		}
	
	
    @Override
    protected void reduce(
        Text key, // TODO: replace Object with input key type
        Iterable<Text> rowTT, // TODO: replace Object with input value type
        Context context) throws IOException, InterruptedException {
    	tableB tab=new tableB(); //struttura
    	iter=0;
    	int mb=1024*1024;
		Runtime runtime = Runtime.getRuntime();
		b++;
		if (((runtime.freeMemory()) / mb)<25) {
			System.out.println("##\nFree memory prima: "+ (runtime.freeMemory()) / mb);
			seen_mod.clear();
			System.out.println("##\nFree memory dopo: "+ (runtime.freeMemory()) / mb);
			}

    	if (key.toString().startsWith("***")) {
    		
    		
			for (Text m: rowTT)
	    	{	context.write(new Text(key), new Text(m));
				
    		}
			
    		}
    	else {
    		String [] head2=key.toString().split("\\*");
    		int deleted = Integer.parseInt(head2[1]);
    		String [] projectionS= head2[0].split(", ");
	    	List<Integer> projectionI = new ArrayList<Integer>();
	    	for (String s: projectionS) projectionI.add(Integer.parseInt(s.trim()));
	    	tab.modify_projection_list(projectionI);

	    	for (Text m: rowTT)
		    	{	

	    			String[] r=m.toString().split("\\|\\|");
	    			int num_item=r.length;
					for (int y=0;y<num_item;y++)
					{
		    		
		    		
		    		String[] parts = r[y].split(",");
					
					
					if (parts.length>1)
					{	String[] transaction_strings=parts[1].split(" ");
						List<Integer> list_integer = new ArrayList<Integer>();
						for(String s : transaction_strings) {
							int transid=Integer.parseInt(s);
							if (transid>dataset_size) dataset_size=transid;
							list_integer.add(transid);
						}
						row_B rowtemp;
						rowtemp = new row_B(parts[0],list_integer);
						tab.add_row(rowtemp);
					}
					else {
						row_B rowtemp;
						List<Integer> listainteri= new ArrayList<Integer>();
						rowtemp = new row_B(parts[0],listainteri);
						tab.add_row(rowtemp);
					}
					tab.add_deleted(deleted);
				}
    		}
	    	recursive(tab,context);}
	    	

    	
    	
    }
}