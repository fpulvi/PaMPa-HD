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
    public static int sent_tables=0;
    public static long startTime;
    public static long stopTime;
   
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
		//context.write(new Text(tabstring), new Text(tabstring2));
		
		//System.out.println(" "+itemset+"\t"+projection_s+"||||"+tabstring+"|*|"+tabstring2);
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
		//clona la proiezione attuale
		List<Integer> projection =new ArrayList<Integer>();
		for (int a: tab.get_projection()) projection.add(a);
		
		//System.out.print("\n rigo 225, proiezione: "+tab.dammi_proiezione_stringa_spazi()+" con eliminate "+tab.dammi_eliminate());
		
		
		
		//1^o Pruning: controllo se potenzialmente è frequente - si deve controllare la lunghezza potenziale più il prefisso già ottenuto finora
		//if (lungh_max_righe<minsup)  // in teoria questo controllo è superfluo
		//{//System.out.print("Ramo da eliminare- 1o pruning");
		//return;
		//}
		// verifico la presenza di itemset ovunque
		 if ((tab.get_list().size()==1) & (tab.max_length()>=minsup)) {
			 
			 	int deleted = tab.get_deleted();
				String itemset = tab.showitemsetString();
				int projection_size= tab.get_projection_size();
				int length_max_rows=tab.max_length();
	        	// ha solo una voce, non ha senso dividerla
	        	int num_row=tab.max_length();
	        	String itemset_complete=tab.showitemsetString();
	        	//context.write(new Text("***"+itemset_complete), new Text(tab.get_projection_string()+"--"+Integer.toString(num_row)));
	        	//System.out.println(itemset_complete+"\t"+tab.get_projection_string()+"||||***"+itemset_complete+"|*|"+tab.get_projection_string()+"--"+Integer.toString(num_row));
	        	context.write(new Text(itemset_complete),new Text(tab.get_projection_string()+"||||***"+itemset_complete+"|*|"+tab.get_projection_string()+"--"+Integer.toString(num_row))); 
	        	//closed_modificata.put(itemset_completo,proiezione+","+row_riga+"--"+Integer.toString(num_row_riga));
	         	//System.out.println("\nLUNGHI 1: ho appena scritto:"+itemset_completo+"||"+tab.dammi_proiezione_stringa_spazi2()+"--"+Integer.toString(num_row_riga));
	        return; }
		
		List<Integer> elements_in_all = tab.elements_in_all(dataset_size);
		if (elements_in_all.size()>0) {
			//System.out.println("\n questi elementi erano ovunque:" +presentiovunque);
			tab.modify_only_list(elements_in_all);
		}
		int deleted = tab.get_deleted();
		String itemset = tab.showitemsetString();
		int projection_size= tab.get_projection_size();
		int lungh_max_righe=tab.max_length();
		
       
        	
		
		//COMPARABLE
				// aggiungo una memoria che tiene conto degli itemset già visti
				// la particolarità di questa memoria è che viene aggiornata con gli itemset più "vecchi"
				// secondo un ordine di tipo depth first nell'esplorazione dell'albero
				//
				//prima controllo se già c'è
				
		if (projection_size>=minsup)	{
			//context.write(new Text("***"+itemset), new Text(tab.get_projection_string()+"--"+Integer.toString(projection_size)));
			//System.out.println(itemset+"\t"+tab.get_projection_string()+"||||***"+itemset+"|*|"+tab.get_projection_string()+"--"+Integer.toString(projection_size));
        	context.write(new Text(itemset),new Text(tab.get_projection_string()+"||||***"+itemset+"|*|"+tab.get_projection_string()+"--"+Integer.toString(projection_size))); 

				//closed_modificata.put(itemset,tab.dammi_proiezione_stringa()+"--"+Integer.toString(lungh_proiezione));
			//System.out.println("\n rigo 276 "+ lungh_proiezione+" ma minsup e' "+minsup+" itemset: "+ itemset);		
		}
		
		// inizio a verificare se chiamare  le altre
		
		// questo vettore serve per il pruning dei valori già analizzati e
		// eliminare le righe che già non avranno abbastanza supp.
		List <Integer> found=new ArrayList();
		
		//per ogni possibile numero di transazione
		for (int i=1;i<=dataset_size;i++) {
			boolean found2=false;
			//inizializzo nuova tabella
			tableB tabtemp=new tableB();
			for (row_B r: tab.get_list()){
				//System.out.print("\naa"+r.mostraitem()+" "+r.mostralista_trans());
				if (r.find_first_and_del((Integer) i)) {
					//se lo contiene
					
					row_B r2= (row_B) r.clone();
					//non dovrebbe piu servire perchè ho snellito piano piano ogni riga
					//for (Integer f: trovati) r2.remove_element((Integer) f);
					if (r2.length()+projection_size+1<minsup) {
						//riga inutile, esco dal for;
						continue;
					}
					found2=true;  //questa riga è utile, la aggiungo
					tabtemp.add_row(r2);
					
				}				
			}
			//ora pensiamo a modicare la proiezione
			if (found2) {
				found.add(i);  // verificare -- mi pare si possa eliminare
			
			tabtemp.add_deleted(deleted);  //eliminate potrebbe essere stato aggiornato
			
			tabtemp.create_projection_modify_list(projection,i);
			//se l'itemset è già visto salto
			//if (visti.contains(tabtemp.mostraitemsetString())) continue;
			
			String itemset_new=tabtemp.showitemsetString();
			String projection_new=tabtemp.get_projection_string_spaces();
			
			if (seen_mod.containsKey(itemset_new)) { // qui non l'aggiungo la combinazione che sto per inviare fra i closed perchè andrei ad aggiungerla 
				// se c'è, devo fare i controlli				//prima ancora di esaminarla nel reducer
				String[] parts2 = ((String) seen_mod.get(itemset_new)).split("--");
				String projection_old= parts2[0];
				int comparison = comparator_f(projection_old,projection_new);
				if (comparison<=0) {//System.out.println("\n317 la nuova comparable ha detto che esiste già: questo itemsset: "+itemset_nuovo+" trovato qui:" +proiezione_nuova+" era già stato trovato qui: "+proiezione_vecchia);
				continue;
				}
				else {seen_mod.put(itemset_new,projection_new+"--"+Integer.toString(projection_size+deleted));
             	//System.out.println("\n321ho appena aggiunto:"+itemset_nuovo+"||"+ proiezione_nuova+"--"+Integer.toString(lungh_proiezione+eliminate));
				//	}
            	}
			}
			else {seen_mod.put(itemset_new,projection_new+"--"+Integer.toString(projection_size+deleted));
			//System.out.println("\n326ho appena aggiunto:"+itemset_nuovo+"||"+ proiezione_nuova+"--"+Integer.toString(lungh_proiezione+eliminate));
				}
			if ((tabtemp.get_list().size()==1) & (tabtemp.max_length()>=minsup)){
				 
			 	int deleted2 = tabtemp.get_deleted();
				String itemset2 = tabtemp.showitemsetString();
				int projection_size2= tabtemp.get_projection_size();
				int length_max_row2=tab.max_length();
	        	// ha solo una voce, non ha senso dividerla
	        	int num_row_row2=tabtemp.max_length();
	        	String itemset_complete2=tabtemp.showitemsetString();
	        	//System.out.println("\n rigo 349"+itemset_completo2+num_row_riga2);
	        	//context.write(new Text("***"+itemset_complete2), new Text(tabtemp.get_projection_string()+"--"+Integer.toString(num_row_row2)));
	        	//System.out.println(itemset_complete2+"\t"+tab.get_projection_string()+"--"+Integer.toString(num_row_row2));
	        	context.write(new Text(itemset_complete2),new Text(tabtemp.get_projection_string()+"||||***"+itemset_complete2+"|*|"+tabtemp.get_projection_string()+"--"+Integer.toString(num_row_row2))); 
	        	
	        	//closed_modificata.put(itemset_completo,proiezione+","+row_riga+"--"+Integer.toString(num_row_riga));
	        		//System.out.println("\nLUNGHI 1_: ho appena scritto:"+itemset_completo2+"||"+tabtemp.dammi_proiezione_stringa_spazi2()+"--"+Integer.toString(num_row_riga2));
	        continue; }
			
			 //serve per fare breadthfirst a livello locale
			//Questa printf stampa la tt che si sta per analizzare
			//System.out.println("\n Questa è la transposed table"+tabtemp.dammi_proiezione()+" che contiene gli itemset "+tabtemp.mostraitemsetString()+" con eliminate: "+tabtemp.dammi_eliminate());
			//for (row r: tabtemp.get_list()) {
			//	System.out.println("\n"+r.mostraitem()+" "+ r.mostralista_trans());				
				//}
			int found3=0;
			int mb=1024*1024;
			Runtime runtime = Runtime.getRuntime();
			stopTime=System.currentTimeMillis();
			//System.out.println("time:"+Long.toString(stopTime-startTime));
			long elapsedTime=(stopTime-startTime)/1000;
			if (elapsedTime>(60*60*2)) print_table(tabtemp,context);
			else {
			if ((((runtime.freeMemory()) / mb)>25)&&(iter<=max_tables)) {recursive(tabtemp,context);}
			else {print_table(tabtemp,context);}}
			tabtemp=null;
			//for (int a=0; a<=br_to_deep;a++)
				//if (projection.contains((Integer) a)) {trovato2=1;}
			
			//if ((trovato2==1)&(iter<=3000)) ricorsiva(tabtemp,context);
			//else {stampa_tabella(tabtemp, context);
			//tabtemp=null;}
			
			}
			
	}
	/*
	for (riga t: tab.dammi_lista()) {
		System.out.print("\nfinale "+t.mostraitem()+" "+ t.mostralista_trans());				
		}
		*/
		return;
			}
    
  
	protected void setup(Context context) throws IOException, InterruptedException
	{
		Heartbeat.createHeartbeat(context);
    	// read the minsup
		minsup = Integer.parseInt(context.getConfiguration().get("minsup"));
		max_tables = Integer.parseInt(context.getConfiguration().get("max_tables"));
		startTime=System.currentTimeMillis();
    	
	}
	
	protected void cleanup (Context context) throws IOException, InterruptedException {
		Heartbeat.stopbeating();
		context.getCounter("sent_tables","sent_tables").increment(sent_tables);
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
           
            if (seen_mod.containsKey(itemset_complete)) { // qui non l'aggiungo la combinazione che sto per inviare fra i closed perchè andrei ad aggiungerla 
            	//System.out.println("\n luckily we are here");// se c'è, devo fare i controlli				//prima ancora di esaminarla nel reducer
				String[] parts2 = ((String) seen_mod.get(itemset_complete)).split("--");
				String projection_old= parts2[0];
				int comparison = comparator_f(projection_old,projection_new);
				if (comparison<=0) {//System.out.println("\n317 la nuova comparable ha detto che esiste già: questo itemsset: "+itemset_nuovo+" trovato qui:" +proiezione_nuova+" era già stato trovato qui: "+proiezione_vecchia);
					//System.out.println("\n luckily we are here e l'ho eliminata: ");
				return;
				}
				else {seen_mod.put(itemset_complete,projection_new+"--"+Integer.toString(projection_size+deleted));
             	//System.out.println("\n321ho appena aggiunto:"+itemset_nuovo+"||"+ proiezione_nuova+"--"+Integer.toString(lungh_proiezione+eliminate));
				//	}
            	}
			}
			else {seen_mod.put(itemset_complete,projection_new+"--"+Integer.toString(projection_size+deleted));
			//System.out.println("\n326ho appena aggiunto:"+itemset_nuovo+"||"+ proiezione_nuova+"--"+Integer.toString(lungh_proiezione+eliminate));
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
            
            recursive (tab,context);
           }
    		else {
    			String[] parts1 = row.split("\t");
    			String oldkey=parts1[0];
    			String oldvalue=parts1[1];
    			String[] parts2= parts1[1].split("--");
    			String itemset_complete=parts1[0].replaceAll("\\*\\*\\*","");
    			String projection=parts2[0];
    			String minsup=parts2[1];
    			//System.out.println("\n sending: "+itemset_complete+" and value: "+projection+"||||"+oldkey+"|*|"+oldvalue);
    			if (seen_mod.containsKey(itemset_complete)) { // it is already among the found itemsets
    				// se c'è, devo fare i controlli				
    				String[] parts3 = ((String) seen_mod.get(itemset_complete)).split("--");  //retrieve the already in memory one
    				String projection_old= parts3[0];  //this is the projection of the oldest
    				int comparison = comparator_f(projection_old,projection);
    				if (comparison<=0) {  //the already in memory was the one to keep
    					//System.out.println("\n317 la nuova comparable ha detto che esiste già: questo itemsset: "+itemset_nuovo+" trovato qui:" +proiezione_nuova+" era già stato trovato qui: "+proiezione_vecchia);
    					//ignore it!
    				}
    				else {
    	                context.write(new Text(itemset_complete),new Text(projection+"||||"+oldkey+"|*|"+oldvalue));   				
    					seen_mod.put(itemset_complete,projection+"--"+minsup);
                 	//System.out.println("\n321ho appena aggiunto:"+itemset_nuovo+"||"+ proiezione_nuova+"--"+Integer.toString(lungh_proiezione+eliminate));
    				//	}
                	}
    			}
    			else {//seen_mod.put(itemset_new,projection_new+"--"+Integer.toString(projection_size+deleted));
    			//System.out.println("\n326ho appena aggiunto:"+itemset_nuovo+"||"+ proiezione_nuova+"--"+Integer.toString(lungh_proiezione+eliminate));
    				context.write(new Text(itemset_complete),new Text(projection+"||||"+oldkey+"|*|"+oldvalue));   				
					seen_mod.put(itemset_complete,projection+"--"+minsup);
    				}
                //context.write(new Text(itemset_complete),new Text(projection+"||||"+oldkey+"|*|"+oldvalue));   
    		}
    		
    }
          
			
			
    }
