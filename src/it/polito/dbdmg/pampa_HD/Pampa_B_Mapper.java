package it.polito.dbdmg.pampa_HD;

/**
 * @author Fabio Pulvirenti
 * @version 0.9
 */



import it.polito.dbdmg.pampa_HD.Pampa_B_Reducer.Heartbeat;
import it.polito.dbdmg.pampa_HD.util.comparator_B;
import it.polito.dbdmg.pampa_HD.util.row_B;
import it.polito.dbdmg.pampa_HD.util.tableB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * WordCount Mapper
 */
class Pampa_B_Mapper extends Mapper<
                   LongWritable, 
                    Text,       
                    Text,        
                    Text> {
    public static int minsup;
    public static int b=0;
    static int dataset_size=0;
    static int iter=0;
    public static int max_tables;
    public static int br_to_deep;
    public static int sent_tables=0;
    public static long startTime=0;
    public static long stopTime;
    static Hashtable seen_mod = new Hashtable<String, String >();
    static Map<String, tableB> tab_input = new HashMap<String, tableB>();
    public static int count_input=0;
    
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
    private static void empty_tab_input(Context context) throws IOException, InterruptedException
    {
    	//prima la ordino per key
    	List<String> keys = new ArrayList(tab_input.keySet());
    	Collections.sort(keys, new Comparator<String>() {
    		
            public int compare(String string1,String string2) {
            	
                return comparator_f(string1,string2);
            }
        });
    	if (startTime==0) startTime=System.currentTimeMillis();
    	//for the moment i just print them
    	 for (String p : keys) {
    		// System.out.println("keys sorted:"+p);
    		 recursive(tab_input.get(p),context);
    	       
    	    }
    	 tab_input.clear();
    }
    
    private static void print_table(tableB tab, Context context ) throws IOException, InterruptedException {
		String tabstring;
		
		int deleted =tab.get_deleted();	
		String projection = tab.get_projection().toString();
		String projection_s= tab.get_projection_string_spaces2();
		String itemset=tab.showitemsetString();
		
		tabstring=projection+"||"+deleted+"*";
		
		String tabstring2="";
		for (row_B r: tab.get_list()) {
			tabstring2=tabstring2+r.as_string()+" ||";
			
		}
		context.write(new Text(" "+itemset),new Text(projection_s+"||||"+tabstring+"|*|"+tabstring2+"|*|"+itemset));
		sent_tables++;

	}
    
    
    public static int comparator_f (String key1, String key2)
	{int comparison=0;
	String[] k1= key1.split(",");
	String[] k2= key2.split(",");
	List<Integer> projection1 = new ArrayList<Integer>();
	List<Integer> projection2 = new ArrayList<Integer>();
	//System.out.println("\n going to process: "+key1.toString()+" e "+key2.toString());
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
		if (projection2.size()>projection1.size()) { //System.out.println("\n"+key1kk.toString()+"comes first of "+key2kk.toString()); 
			return -1;}
		else if (projection1.size()>projection2.size()){  //System.out.println("\n"+key2kk.toString()+"comes first of "+key1kk.toString()); 
			return 1;}
		else {
			return 0;}
		}
	}
    
    private static void recursive (tableB tab, Context context) throws IOException, InterruptedException
	{ 	iter++;
		List<Integer> projection =new ArrayList<Integer>();
		for (int a: tab.get_projection()) projection.add(a);

		if ((tab.get_list().size()==1)) {
			   	int num_row=tab.max_length();
	        	String itemset_complete=tab.showitemsetString();
	        	String projection_new = tab.get_projection_string();
	        	
	        	if (seen_mod.containsKey(itemset_complete)) {
					String[] parts2 = ((String) seen_mod.get(itemset_complete)).split("--");
					String projection_old= parts2[0];
					int comparison = comparator_f(projection_old,projection_new);
					if (comparison<=0) {
					}
					else {seen_mod.put(itemset_complete,projection_new+"--"+Integer.toString(num_row));
	            	}
				}
				else {seen_mod.put(itemset_complete,projection_new+"--"+Integer.toString(num_row));
				}
	        	return;
		}
		List<Integer> elements_in_all = tab.elements_in_all(dataset_size);
		if (elements_in_all.size()>0) {
			tab.modify_only_list(elements_in_all);
		}
		
		String itemset = tab.showitemsetString();
		int projection_size= tab.get_projection_size();

			String projection_new =  tab.get_projection_string();
			//context.write(new Text("***"+itemset), new Text(tab.get_projection_string()+"--"+Integer.toString(projection_size)));
			//System.out.println(itemset+"\t"+tab.get_projection_string()+"||||***"+itemset+"|*|"+tab.get_projection_string()+"--"+Integer.toString(projection_size));
        	//context.write(new Text(itemset),new Text(tab.get_projection_string()+"||||***"+itemset+"|*|"+tab.get_projection_string()+"--"+Integer.toString(projection_size))); 
        	
        	if (seen_mod.containsKey(itemset)) {
				String[] parts2 = ((String) seen_mod.get(itemset)).split("--");
				String projection_old= parts2[0];
				int comparison = comparator_f(projection_old,projection_new);
				if (comparison<=0) {//System.out.println("\n317 la nuova comparable ha detto che esiste già: questo itemsset: "+itemset_nuovo+" trovato qui:" +proiezione_nuova+" era già stato trovato qui: "+proiezione_vecchia);

				}
				else {seen_mod.put(itemset,projection_new+"--"+Integer.toString(projection_size));
            	}
			}
			else {seen_mod.put(itemset,projection_new+"--"+Integer.toString(projection_size));
			//System.out.println("\n326ho appena aggiunto:"+itemset_nuovo+"||"+ proiezione_nuova+"--"+Integer.toString(lungh_proiezione+eliminate));
				}
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
			
			tabtemp.add_deleted(tab.get_deleted());
			
			tabtemp.create_projection_modify_list(projection,i);

			
			String itemset_new=tabtemp.showitemsetString();
			projection_new=tabtemp.get_projection_string_spaces();
			
			if (seen_mod.containsKey(itemset_new)) {
				String[] parts2 = ((String) seen_mod.get(itemset_new)).split("--");
				String projection_old= parts2[0];
				int comparison = comparator_f(projection_old,projection_new);
				if (comparison<=0) {//System.out.println("\n317 la nuova comparable ha detto che esiste già: questo itemsset: "+itemset_nuovo+" trovato qui:" +proiezione_nuova+" era già stato trovato qui: "+proiezione_vecchia);
				continue;
				}
				else {
            	}
			}
			else {
				}

			if (tabtemp.get_list().size()==1) {
			 	
	        	int num_row_row2=tabtemp.max_length();
	        	String itemset_complete2=tabtemp.showitemsetString();

				seen_mod.put(itemset_complete2,tabtemp.get_projection_string()+"--"+Integer.toString(num_row_row2));
				continue; }
			

			int found3=0;
			int mb=1024*1024;
			Runtime runtime = Runtime.getRuntime();
			stopTime=System.currentTimeMillis();

			long elapsedTime=(stopTime-startTime)/1000;
			
			if (elapsedTime>(60*60*1)) print_table(tabtemp,context);
			else {
			if ((((runtime.freeMemory()) / mb)>0)&&(iter<=max_tables)) {recursive(tabtemp,context);}
			else {print_table(tabtemp,context);}}
			tabtemp=null;

		
			}
			
	}

		return;
			}
    
  
	protected void setup(Context context) throws IOException, InterruptedException
	{
		Heartbeat.createHeartbeat(context);
    	// read the minsup
		minsup = Integer.parseInt(context.getConfiguration().get("minsup"));
		max_tables = Integer.parseInt(context.getConfiguration().get("max_tables"));
		br_to_deep = Integer.parseInt(context.getConfiguration().get("br_to_deep"));
		
	}
	
	protected void cleanup (Context context) throws IOException, InterruptedException {
		empty_tab_input(context);
		Heartbeat.stopbeating();
		context.getCounter("sent_tables","sent_tables").increment(sent_tables);
		Enumeration e = seen_mod.keys();
		Object ciao;
		while (e.hasMoreElements()) {	
			String key = (String) e.nextElement();
			String temp = (String) seen_mod.get(key);
			temp = (String) seen_mod.get(key);
			
			String[] parts = ((String) seen_mod.get(key)).split("--");
			if (Integer.parseInt(parts[1])>=minsup) {
			context.write(new Text(key), new Text(parts[0]+"||||***"+key+"|*|"+parts[0]+"--"+parts[1]));
			}
			}
		}
	
    @Override
    protected void map(
            LongWritable key,   
            Text value,        
            Context context) throws IOException, InterruptedException {
    		String row=value.toString();
    		//if (row.startsWith("*")) {return;}
	    	
			b++;
			
    		if (!row.startsWith("*")) {
    			String [] row_itemset=row.split("\\|\\*\\|");
    		//System.out.println("\ntable: "+row);
    		String [] rowS = row_itemset[0].split("\\*\t");
            //analyse the header
    		String [] head2 = rowS[0].split("\\|\\|");
    		String projection = head2[0].replaceAll("\\[","").replaceAll("\\]","");            
            int deleted=Integer.parseInt(head2[1].trim());
            String [] projectionS= projection.split(",");
            List<Integer> projectionI= new ArrayList();
            for (String s: projectionS) projectionI.add(Integer.parseInt(s.trim()));
            String itemset = "";
            List<String> itemsetS1 = new ArrayList();
            List<String> itemsetS = new ArrayList();
            String [] tables = rowS[1].split("\\|\\|");
            String itemset_complete="";
            String rowR="";//not used
            int num_rowR=0;//not used
            tableB tab= new tableB();
            tab.modify_projection_and_list(projectionI);
            tab.add_deleted(deleted);
            int projection_size= tab.get_projection_size();  
            itemset_complete=row_itemset[1];
           // System.out.println("Analyzing: "+itemset_complete);
            String projection_new=projection;
           
            if (seen_mod.containsKey(itemset_complete)) { String[] parts2 = ((String) seen_mod.get(itemset_complete)).split("--");
				String projection_old= parts2[0];
				int comparison = comparator_f(projection_old,projection_new);
				if (comparison<=0) {
				return;
				}
				
			}
			for (int f=0;f<tables.length;f++) {
            	row_B rowtemp;										
            	String [] row2= tables[f].split(",");
            	String[] transactions_string=row2[1].split(" ");
				List<Integer> list_integer = new ArrayList<Integer>();
				for(String s : transactions_string) {
					int transid=Integer.parseInt(s);
					if (transid>dataset_size) dataset_size=transid;
					list_integer.add(transid);
				}
            	itemsetS1.add(row2[0]);
            	rowtemp = new row_B(row2[0],list_integer);
            	tab.add_row(rowtemp);
            	
            }

            tab_input.put(projection_new, tab);
            count_input++;

           }
    		else {
    			//String[] parts1 = row.split("\t");
    			String [] row_itemset=row.split("\\|\\*\\|");
    			String[] parts1 = row_itemset[0].split("\t");
    			String oldkey=parts1[0];
    			String oldvalue=parts1[1];
    			String[] parts2= parts1[1].split("--");
    			String itemset_complete=parts1[0].replaceAll("\\*\\*\\*","");
    			String projection=parts2[0];
    			String minsup=parts2[1];
    			if (seen_mod.containsKey(itemset_complete)) { // it is already among the found itemsets

    				String[] parts3 = ((String) seen_mod.get(itemset_complete)).split("--");  //retrieve the already in memory one
    				String projection_old= parts3[0];  //this is the projection of the oldest
    				int comparison = comparator_f(projection_old,projection);
    				if (comparison<=0) {
    				}
    				else {

    					seen_mod.put(itemset_complete,projection+"--"+minsup);

                	}
    			}
    			else {
					seen_mod.put(itemset_complete,projection+"--"+minsup);
    				}

    		}
    		
    }
          
			
			
}
