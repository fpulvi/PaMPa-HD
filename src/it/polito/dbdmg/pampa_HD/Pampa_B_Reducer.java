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
		//context.write(new Text("*"+tab.dammi_proiezione().toString()), new Text(sizes));
		
		// separatore da intestazione *
		// all'interno dell'intestazione, separatore proiezione da lunghezza tabella ||
		// all'interno della tabella, separatore righe fra di loro ||
		// all'interno della riga, separatore item da transazioni ,
		String tabstring2="";
		//System.out.println("\n Questa è la transposed table"+tabtemp.dammi_proiezione()+" che contiene gli itemset "+tabtemp.mostraitemsetString());
		for (row_B r: tab.get_list()) {
			tabstring2=tabstring2+r.as_string()+" ||";
			
			
			//System.out.println("\n"+r.mostraitem()+" "+ r.mostralista_trans());				
			}
		context.write(new Text(tabstring), new Text(tabstring2));
	}
	
	
/*

	private static void ricorsiva_1giro (tabella tab)
	{
	 	
			tabella storage= new tabella();
			//1^o controllo se potenzialmente è frequente
			if (tab.massimalunghezza()<minsup)
			{System.out.print("Ramo da eliminare - 1o pruning");
			return;
			}
			
			//2o Pruning
			List<Integer> presentiovunque = tab.elementiovunque(numerorighedataset);
			System.out.print("\necco la proiezione"+tab.dammi_proiezione());
			if (tab.elementiovunque(numerorighedataset).size()>0) {
				System.out.print("\n questi elementi erano ovunque:" +tab.elementiovunque(numerorighedataset));
				tab.modifica_proiezione_e_lista_sololista(tab.elementiovunque(numerorighedataset));
				System.out.print("\nAllora la tabella diventa "+tab.dammi_proiezione());
				ricorsiva(tab);
				return;}
			
			//3o Pruning qui non ha senso come primo giro
			if (visti.contains(tab.mostraitemsetString())) {
				//System.out.print("\n itemset già visto, salto");
				return;
			}
			else
				visti.add(tab.mostraitemsetString());
			
			//se il supporto è superiore o uguale al minsup lo aggiungo ai closed
			if (tab.is_closed(minsup)) {
				//System.out.print("\n ho a che fare con "+tab.mostraitemsetString()+ " con supporto "+projection.size());
				if (closed.containsKey(tab.mostraitemsetString()))
						{if ((Integer) closed.get(tab.mostraitemsetString())<=tab.dammi_proiezione().size()) {
							//System.out.print("\nBIS ho a che fare con "+tab.mostraitemsetString()+ " con supporto "+projection.size());
							closed.remove(tab.mostraitemsetString());
							closed.put(tab.mostraitemsetString(), tab.dammi_proiezione().size());}
						}
				else closed.put(tab.mostraitemsetString(), tab.dammi_proiezione().size());
				}
			
			
			//inizio a chiamare le altre con la ricorsione
			
			//per ogni possibile numero di transazione
			for (int i=1;i<=numerorighedataset;i++) {
				boolean trovato=false;
				tabella tabtemp=new tabella();
				for (riga r: tab.dammi_lista()){
					//System.out.print("\naa"+r.mostraitem()+" "+r.mostralista_trans());
					if (r.mostralista_trans().contains((Integer) i)) {
						//se lo contiene
						trovato=true;
						
						
						riga s=(riga) r.clone();
						tabtemp.add_riga(s);
						//qui andrebbe un break per non continuare a controllare la riga;
						//System.out.print("\nHo appena aggiunto la riga "+r.mostraitem()+" "+r.mostralista_trans()+" perchè è presente la riga "+i);
					}				
				}
				//ora pensiamo al prefisso/proiezione
				if (trovato) {
					List<Integer> projection =new ArrayList<Integer>();
					projection.add(i);
					//System.out.print ("\nSto per modificare "+projection);
					tabtemp.modifica_proiezione_e_lista(projection);
					//System.out.print("\n Questa è la transposed table"+tabtemp.dammi_proiezione()+" che contiene gli itemset "+tabtemp.mostraitemsetString());
					for (riga r: tabtemp.dammi_lista()) {
						//System.out.print("\n"+r.mostraitem()+" "+ r.mostralista_trans());				
						}
					ricorsiva(tabtemp);
					}
				
			
			return;
	}
	*/
	/////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////
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
		if (projection2.size()>projection1.size()) { //System.out.println("\n2per me "+key1kk.toString()+"viene prima di "+key2kk.toString()); 
			return -1;}
		else if (projection1.size()>projection2.size()){  //System.out.println("\n2per me "+key2kk.toString()+"viene prima di "+key1kk.toString()); 
			return 1;}
		else { //System.out.println("\n sono finito qui?"); 
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
	        	context.write(new Text("***"+itemset_complete), new Text(tab.get_projection_string()+"--"+Integer.toString(num_row)));
	        	
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
		int length_max_rows=tab.max_length();
		
       
        	
		
		//COMPARABLE
				// aggiungo una memoria che tiene conto degli itemset già visti
				// la particolarità di questa memoria è che viene aggiornata con gli itemset più "vecchi"
				// secondo un ordine di tipo depth first nell'esplorazione dell'albero
				//
				//prima controllo se già c'è
				
		if (projection_size>=minsup)	
		context.write(new Text("***"+itemset), new Text(tab.get_projection_string()+"--"+Integer.toString(projection_size)));
				//closed_modificata.put(itemset,tab.dammi_proiezione_stringa()+"--"+Integer.toString(lungh_proiezione));
		
		
		
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
				String proiezione_vecchia= parts2[0];
				int confronto = comparator_f(proiezione_vecchia,projection_new);
				if (confronto<=0) {//System.out.println("\n317 la nuova comparable ha detto che esiste già: questo itemsset: "+itemset_nuovo+" trovato qui:" +proiezione_nuova+" era già stato trovato qui: "+proiezione_vecchia);
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
	        	//if (num_row_row2>=30)
        			//System.out.println("\n rigo 349"+itemset_completo2+num_row_riga2);
	        	//context.write(new Text("***"+itemset_complete2), new Text(tabtemp.get_projection_string()+"--"+Integer.toString(num_row_row2)));
	        	//closed_modificata.put(itemset_completo,proiezione+","+row_riga+"--"+Integer.toString(num_row_riga));
	         	//System.out.println("\nLUNGHI 1_: ho appena scritto:"+itemset_completo2+"||"+tabtemp.dammi_proiezione_stringa_spazi2()+"--"+Integer.toString(num_row_riga2));
	        continue; }
			
			 //serve per fare breadthfirst a livello locale
			//Questa printf stampa la tt che si sta per analizzare
			//System.out.println("\n Questa è la transposed table"+tabtemp.dammi_proiezione()+" che contiene gli itemset "+tabtemp.mostraitemsetString()+" con eliminate: "+tabtemp.dammi_eliminate());
			//for (riga r: tabtemp.dammi_lista()) {
				//System.out.println("\n"+r.mostraitem()+" "+ r.mostralista_trans());				
				//}
			int found3=0;
			int mb=1024*1024;
			Runtime runtime = Runtime.getRuntime();
			if (((runtime.freeMemory()) / mb)<50) seen_mod.clear();
			if ((((runtime.freeMemory()) / mb)>25)&&(iter<=max_tables)) {recursive(tabtemp,context);}
			else {print_table(tabtemp,context);
				}
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
	
	/*
protected void stampa_closed(Context context) throws IOException, InterruptedException {
    	
    	Enumeration keys = closed_modificata.keys();
		int a = 1;
		String closed_key="";
		String closed_value="";
		while (keys.hasMoreElements()) 
		{  
			Object key2 = keys.nextElement();
			String s= (String) closed_modificata.get(key2);
			String[] s2=s.split("--");
			
			if ((Integer.parseInt(s2[1].trim()))<minsup) continue;
			closed_key=closed_key+key2.toString()+" ||";
			closed_value=closed_value+closed_modificata.get(key2)+" ||";
			if ((a%100==0)) {
				
				
				//System.out.println("\n*** "+closed_key+" - "+closed_value);
				context.write(new Text("***"+closed_key), new Text(closed_value));
				closed_key="";
				closed_value="";
			}
			 a++;
			    		//bw.write("\n* "+key.toString()+" - "+closed.get(key));
		}
		//System.out.println("\n*** "+closed_key+" - "+closed_value);
		if(!closed_key.equals("")) context.write(new Text("***"+closed_key), new Text(closed_value));
		//System.out.println("ciaooooooooooooooooooooooooo");
	}
	
	
*/
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
		/*
		Enumeration keys = closed_modificata.keys();
		int a = 1;
		String closed_key="";
		String closed_value="";
		while (keys.hasMoreElements()) 
		{  
			Object key2 = keys.nextElement();
			String s= (String) closed_modificata.get(key2);
			String[] s2=s.split("--");
			
			if ((Integer.parseInt(s2[1].trim()))<minsup) continue;
			closed_key=closed_key+key2.toString()+" ||";
			closed_value=closed_value+closed_modificata.get(key2)+" ||";
			if ((a%100==0)) {
				
				
				//System.out.println("\n*** "+closed_key+" - "+closed_value);
				context.write(new Text("***"+closed_key), new Text(closed_value));
				closed_key="";
				closed_value="";
			}
			 a++;
			    		//bw.write("\n* "+key.toString()+" - "+closed.get(key));
		}
		//System.out.println("\n*** "+closed_key+" - "+closed_value);
		if(!closed_key.equals("")) context.write(new Text("***"+closed_key), new Text(closed_value));
		//System.out.println("ciaooooooooooooooooooooooooo");
	*/}
	
	
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
        // TODO: implement here Reduce code
        //int occurrances = 0;
       // for (IntWritable rigatemp : righe) {
       //     occurrances += rigatemp.get();
       // }
       // context.write(key, new IntWritable(occurrances));
    	
    	//System.out.println("\n mi è arrivato "+key.toString()+" | "+rigaTT.toString());
    	//costruzione tabella
    	if (key.toString().startsWith("***")) {
    		
    		
			for (Text m: rowTT)
	    	{	context.write(new Text(key), new Text(m));
				
    		}
			
    		}
    	else {
	    	//System.out.println("\n 411mi è arrivato questo "+key.toString());
    		String [] head2=key.toString().split("\\*");
    		int deleted = Integer.parseInt(head2[1]);
    		String [] projectionS= head2[0].split(", ");
	    	List<Integer> projectionI = new ArrayList<Integer>();
	    	for (String s: projectionS) projectionI.add(Integer.parseInt(s.trim()));
	    	tab.modify_projection_list(projectionI);
	    	//System.out.println("\n ho caricato in memoria la tabella relativa alla chiave "+proiezioneI.toString());
	    	
	    	for (Text m: rowTT)
		    	{	
					//numerorighedataset++;  questo non vale piu in questo caso
	    		
	    			String[] r=m.toString().split("\\|\\|");
	    			int num_item=r.length;
	    			//System.out.println("\n420 mi è arrivato "+key.toString()+" | "+m.toString()+" con num_item= "+num_item);
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
						//Collections.sort(listainteri);
						row_B rowtemp;
						//System.out.println("\nn:"+y+" "+key.toString()+"||"+parts[0]+","+listainteri.toString());
						rowtemp = new row_B(parts[0],list_integer);
						tab.add_row(rowtemp);
					}
					else {
						row_B rowtemp;
						List<Integer> listainteri= new ArrayList<Integer>();
						rowtemp = new row_B(parts[0],listainteri);
						tab.add_row(rowtemp);
						//System.out.println("\nn:"+y+" "+key.toString()+"||"+parts[0]+",sono fuori non so perchè, r era:"+r[y]);
					}
					tab.add_deleted(deleted);
				}
    		}
	    	// inizia Carpenter
	    	recursive(tab,context);}
	    	
    	//visti.clear();  // visto che arrivano in ordine questa la possiamo sfruttare
    	
    	//System.out.println("\n ehiiii siamo alla fine");
    	
    	//closed.clear();
    	
    	
    }
}