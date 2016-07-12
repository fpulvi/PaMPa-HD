package it.polito.dbdmg.pampa_HD;

/**
 * @author Fabio Pulvirenti
 * @version 0.9
 */






import it.polito.dbdmg.pampa_HD.util.comparator_A;
import it.polito.dbdmg.pampa_HD.util.row_A;
import it.polito.dbdmg.pampa_HD.util.tableA;

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

 */
class Pampa_A_Reducer extends Reducer<Text,Text,Text,Text> 
{
	static int minsup;
	static int max_tables;
//	static Hashtable closed = new Hashtable<String, Integer>();
//	static Hashtable closed_modified = new Hashtable<String, String >();
	static int dataset_size=0;
//	static int b=0;
//	static List<String> seen = new ArrayList<String>();
	static int iter=0;
	static Hashtable seen_mod = new Hashtable<String, String >();
	public static int sent_tables=0;
	
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
	
	
	private static void print_table(tableA tab, Context context ) throws IOException, InterruptedException {
		String tabstring;
		int size = tab.get_list().size();
		int deleted =tab.get_deleted();
		String sizes= Integer.toString(size);
		tabstring=tab.get_projection().toString()+"||"+deleted+"*";
		//context.write(new Text("*"+tab.dammi_proiezione().toString()), new Text(sizes));
		
		// header separator *
				//within header, separator projection with table legnth ||
				//within table, row separator ||
				//within row, separator item and transactions ,
		String tabstring2="";
		for (row_A r: tab.get_list()) {
			tabstring2=tabstring2+r.as_string()+" ||";
			}
		context.write(new Text(tabstring), new Text(tabstring2));
		sent_tables++;
	}

	private static void recursive (tableA tab, Context context) throws IOException, InterruptedException
	{ 	iter++;
	//clone the current projection
	List<Integer> projection =new ArrayList<Integer>();
	for (int a: tab.get_projection()) projection.add(a);
	if ((tab.get_list().size()==1) & (tab.max_length()>=minsup)) {		 
		 	int deleted = tab.get_deleted();
			String itemset = tab.showitemsetString();
			int projection_size= tab.get_projection_size();
			int length_max_rows=tab.max_length();
        	// only one entry!
        	int num_row=tab.max_length();
        	String itemset_complete=tab.showitemsetString();
        	context.write(new Text("***"+itemset_complete), new Text(tab.get_projection_string()+"--"+Integer.toString(num_row)));
        	// if (num_row>=30) fabio1
    			//System.out.println("\n rigo 247"+itemset_completo+num_row_riga);
        	//closed_modificata.put(itemset_completo,proiezione+","+row_riga+"--"+Integer.toString(num_row_riga));
         	//System.out.println("\nLUNGHI 1: ho appena scritto:"+itemset_completo+"||"+tab.dammi_proiezione_stringa_spazi2()+"--"+Integer.toString(num_row_riga));
        //return;
        	}
	
	List<Integer> elements_in_all= tab.elements_in_all(dataset_size);
	if (elements_in_all.size()>0) {
		//elements in all the lists
		tab.modify_only_list(elements_in_all);
	}
	int deleted = tab.get_deleted();
	String itemset = tab.showitemsetString();
	int projection_size= tab.get_projection_size();
	int length_max_rows=tab.max_length();
	if (projection_size>=minsup)	
	context.write(new Text("***"+itemset), new Text(tab.get_projection_string()+"--"+Integer.toString(projection_size)));
			//closed_modificata.put(itemset,tab.dammi_proiezione_stringa()+"--"+Integer.toString(lungh_proiezione));
	//if (lungh_proiezione>=30)
		//System.out.println("\n rigo 276"+itemset+lungh_proiezione);		
	
	
	// start to verify to call the other
	//this vector is used to prune the values already seen and delete the rows too short
	List <Integer> found=new ArrayList();	
	//for each possible tid
	for (int i=1;i<=dataset_size;i++) {
		boolean found2=false;
		//new table
		tableA tabtemp=new tableA();
		for (row_A r: tab.get_list()){
			//System.out.print("\naa"+r.mostraitem()+" "+r.mostralista_trans());
			if (r.find_first_and_del((Integer) i)) {
				//if it contains
				
				row_A r2= (row_A) r.clone();
				//should not be needed because emptied one by one the row
				//for (Integer f: trovati) r2.remove_element((Integer) f);
				if (r2.length()+projection_size+1<minsup) {
					//useless, go out!
					continue;
				}
				found2=true;  //useful, add it!
				tabtemp.add_row(r2);
				
			}				
		}
		//modify the projection
		if (found2) {
			found.add(i);  // should be useless - verify
		tabtemp.add_deleted(deleted);  //deleted could be updated
		tabtemp.create_projection_modify_list(projection,i);
		String itemset_new=tabtemp.showitemsetString();
		String projection_new=tabtemp.get_projection_string_spaces();
		
		if (seen_mod.containsKey(itemset_new)) { //i don't add it here because it should be examined in the reducer
			String[] parts2 = ((String) seen_mod.get(itemset_new)).split("--");
			String projection_old= parts2[0];
			int comparison = comparator_f(projection_old,projection_new);
			if (comparison<=0) {//System.out.println("\n this "+itemset_nuovo+" found here:" +proiezione_nuova+" already found here: "+proiezione_vecchia);
			continue;
			}
			else {seen_mod.put(itemset_new,projection_new+"--"+Integer.toString(projection_size+deleted));
         	
        	}
		}
		else {seen_mod.put(itemset_new,projection_new+"--"+Integer.toString(projection_size+deleted));
		
			}
		if ((tabtemp.get_list().size()==1) & (tabtemp.max_length()>=minsup)){
		 	int deleted2 = tabtemp.get_deleted();  //should not be useful, check
			String itemset2 = tabtemp.showitemsetString();
			int projection_size2= tabtemp.get_projection_size();
			int length_max_row2=tab.max_length();
        	// only one entry!
        	int num_row_row2=tabtemp.max_length();
        	String itemset_complete2=tabtemp.showitemsetString();        	
        	context.write(new Text("***"+itemset_complete2), new Text(tabtemp.get_projection_string()+"--"+Integer.toString(num_row_row2)));
        	continue; }
		
		 
		//This printf prints everything is going to be processed
		//System.out.println("\n Questa è la transposed table"+tabtemp.dammi_proiezione()+" che contiene gli itemset "+tabtemp.mostraitemsetString()+" con eliminate: "+tabtemp.dammi_eliminate());
		//for (riga r: tabtemp.dammi_lista()) {
			//System.out.println("\n"+r.mostraitem()+" "+ r.mostralista_trans());				
			//}
			int found3=0;
			int mb=1024*1024;
			Runtime runtime = Runtime.getRuntime();
			if (((runtime.freeMemory()) / mb)<25) seen_mod.clear();
			if ((((runtime.freeMemory()) / mb)>25)&&(iter<=max_tables)) {recursive(tabtemp,context);}
			else {print_table(tabtemp,context);
				}
			tabtemp=null;		
			}
		}
		return;
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
	
		for (int b=0; (b<projection1.size())&(b<projection2.size());b++) {
			comparison=Integer.compare(projection1.get(b),projection2.get(b));
			if (comparison!=0) return comparison;
			}
		if (projection2.size()>projection1.size()) { //System.out.println("\n2per me "+key1kk.toString()+"viene prima di "+key2kk.toString()); 
			return -1;}
		else if (projection1.size()>projection2.size()){  //System.out.println("\n2per me "+key2kk.toString()+"viene prima di "+key1kk.toString()); 
			return 1;}
		else {  
			return 0;}
		}
	}
	
	
	protected void setup(Context context) throws IOException, InterruptedException
	{
    	// read the minsup
		Heartbeat.createHeartbeat(context);
		minsup = Integer.parseInt(context.getConfiguration().get("minsup"));
		max_tables = Integer.parseInt(context.getConfiguration().get("max_tables"));
    	
	}
	
	protected void cleanup (Context context) throws IOException, InterruptedException {
		Heartbeat.stopbeating();
		context.getCounter("sent_tables","sent_tables").increment(sent_tables);

	}
	 
	
    @Override
    protected void reduce(
        Text key, 
        Iterable<Text> rowTT, 
        Context context) throws IOException, InterruptedException {
    	tableA tab=new tableA(); //struct
    	String key_s = key.toString();
    	tab.modify_projection(Integer.parseInt(key_s));
    	for (Text r: rowTT)
    	{	//System.out.println("\n just arrived "+key.toString()+" | "+r.toString());
			//numerorighedataset++;  useless now
			String[] parts = r.toString().split(",");
			if (parts.length>1)
			{	String[] transaction_strings=parts[1].split(" ");
				List<Integer> list_integer = new ArrayList<Integer>();;
				for(String s : transaction_strings) {
					int transid=Integer.parseInt(s);
					if (transid>dataset_size) dataset_size=transid;
					list_integer.add(transid);
				}
				row_A rowtemp;
				rowtemp = new row_A(parts[0],list_integer);
				tab.add_row(rowtemp);
			}
			else {
				row_A rowtemp;
				List<Integer> listainteri= new ArrayList<Integer>();
				rowtemp = new row_A(parts[0],listainteri);
				tab.add_row(rowtemp);
			}
			
		}
    	// start Carpenter
    	recursive(tab, context);
    	
    }
}