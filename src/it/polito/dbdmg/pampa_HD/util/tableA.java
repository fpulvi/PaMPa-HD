package it.polito.dbdmg.pampa_HD.util;

import it.polito.dbdmg.pampa_HD.util.row_A;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;



public class tableA {
	
	public static int data_length=100;
	// item contenuti
	private List<String> itemset =new ArrayList<String>();;
	// lista righe
	private List<row_A> rows=new ArrayList<row_A>();
	//transposed table id / proiezione
	private List<Integer> projection = new ArrayList<Integer>();
	//numero righe eliminate ma da considerare nel conteggio del supporto
	private int deleted=0;
	
	//costruttore proiezione 
	public void modify_projection (Integer projection) {
		this.projection.add(projection);
	}
	public void modify_projection_list (List<Integer> projection) {  //not used in A
		this.projection.addAll(projection);
	}
	
	//costruttore copia
	//public tabella (tabella tab1) {
		//itemset=tab1.mostraitemsetList();
		//righe=tab1.dammi_lista();
		//projection=tab1.dammi_proiezione();
	//}
	//costruttore vuoto
	//public tabella () {}
	
	//costruttore con lista righe - mai usato
	/*public tabella (List<riga> righe) {
		this.righe=righe;
		for(riga r : righe) this.itemset.add(r.mostraitem());
	}
	*/
	
	//metodo per popolare tabella
	public void add_row (row_A r) {
		this.rows.add(r);		
		this.itemset.add(r.showitem());
		Collections.sort(itemset);
	}
	
	//metodo per restituire la lista degli items come lista
	public List<String> showitemsetList () {  //not used in a
		return itemset;
	}
	
	//metodo per restituire la lista degli items come stringa
	public String showitemsetString () {
		String itemset_s=new String();
		for (String s: itemset) itemset_s=itemset_s+" "+s;
		return itemset_s;
	}
	
	//metodo per restituire la proiezione attuale
	public List<Integer> get_projection() {  
		return projection;
	}
	
	//metodo per restiture la proiezione attuale come stringa
		public String get_projection_string () {
			String string1="";
			for (Integer s: projection) string1=string1+s.toString()+",";
			return string1;
			
		}
	//metodo per restiture la proiezione attuale come stringa con spazi
			public String get_projection_string_spaces () {
				String string1="";
				for (Integer s: projection) string1=string1+" "+s.toString()+",";
				return string1;
				
			}
	//metodo per restiture la proiezione attuale come stringa con spazi // follia della virgola finale!
	public String get_projection_string_spaces2 () {  //not used in A
		String string1="";
		int n=0;
		for (Integer s: projection) {
			if (n==0) string1=s.toString();
			else string1=string1+", "+s.toString();
			n=1;
		}
		return string1;
		
	}
	
	//metodo per ottenere la lunghezza della proiezione attuale
		public Integer get_projection_size() {
			return (projection.size()+deleted);
		}
	
	//metodo per modificare la proiezione
	public void modify_projection_and_list2(int i) {  //not used in A
		this.projection.add(i);
		for (row_A r: this.rows)
			for (int a=0; a<=i; a++) {
				r.remove_element((Integer) a);
			}	
	}
		
	//*****caso pruning 2 -- metodi aggiornamento tabella
	//metodo per aggiornare la proiezione attuale
	public void modify_projection_and_list (List<Integer> l) {  //not used in A
		this.projection.addAll(l);
		for (int i: l)
			for (row_A r: this.rows)
				for (int a=0; a<=i; a++) {
					r.remove_element((Integer) a);
				}
	}
	// questo metodo crea la proiezione, visto che siamo in una nuova tabella
	// quando viene chiamato, e modifica la lista solo togliendo i. //da verificare se serve
	public void create_projection_modify_list(List<Integer> l, int i) {
		this.projection.addAll(l);
		this.projection.add(i);
		for (row_A r: this.rows)
			for (int a=0; a<=i; a++) {
				r.remove_element((Integer) a);
			}
				
	}
	
	//metodo per aggiornare la proiezione attuale
		public void modify_only_list(List<Integer> l) {
			for (int a=0;a<l.size();a++) {		
				deleted++;
			}
			for (int i: l)
				for (row_A r: this.rows)
					{
					r.remove_element((Integer) i);
					}
		}
	
	//metodo per trovare la minima lunghezza delle righe -- inutile
	public int minimum_length() {  //not used in A
		int min=rows.get(0).length();
		for (row_A r: rows) if (r.length()<min) min=r.length();
		return min;		
	}
	
	//metodo per trovare la massima lunghezza delle righe
		public int max_length() {
			int max=0;
			for (row_A r: rows) if (r.length()>max) max=r.length();
			return max+this.get_projection_size();		
		}
	
	//metodo per decidere se aggiungere l'itemset ai closed
	public boolean is_closed(int minsup) {
		if (projection.size()+deleted>=minsup)
			return true;
		else return false;
	}
	
	//metodo per vedere se ci sono elementi presenti ovunque
	public List<Integer> elements_in_all(int row_number) {
		List<Integer> omnipresent=new ArrayList();
		int a=0;
		for (int i=1;i<=row_number;i++) {
			for (row_A r:rows) if (r.showlist_trans().contains(i)) a++;
			if (a>=rows.size()) omnipresent.add(i);
			a=0;}
		return omnipresent;
	}
	
	//metodo per vedere se ci sono elementi presenti ovunque ma solo alle prime posizioni
	//alla fine mai usata	
	public List<Integer> elements_in_all_first(int row_number) {   //not used in A
			List<Integer> omnipresent=new ArrayList();
			int a=0;
			boolean stop=true;
			int o= 0;
			for (int i=1;i<=row_number;i++) {
				//System.out.print("\n rispetto questo itemset" +this.mostraitemsetString());
				for (row_A r:rows) {
					//System.out.print("\n" +r.mostralista_trans());
					if (r.showlist_trans().size()>0) {
						if (r.showlist_trans().get(o)==i) a++;
					}
				}
				if (a>=rows.size()) {
					omnipresent.add(i);
					o++;
				}
				else break;
				a=0;}
			return omnipresent;
		}
		

	//metodo per vedere se ci sono elementi presenti ovunque ma solo alle prime posizioni
		public List<Integer> elements_in_all_first_backup() {
			List<Integer> omnipresent=new ArrayList();
			int a=0;
			boolean stop=true;
			for (int i=6;i<1000;i++) {
				for (row_A r:rows) if (r.showlist_trans().contains(i)) a++;
				if (a>=rows.size()) 
					{omnipresent.add(i);
					 stop=false;
					}
				if (stop==true) break;
				a=0;
				}
			return omnipresent;
		}
	
	//metodo per lavorare sulla lista
	public List<row_A> get_list() {
		return rows;
	}
	//metodo per settare le eliminate alla creazione
	public void add_deleted (int deleted_t) {
		this.deleted=deleted_t;
	}
	//metodo per restituire le eliminate (ereditarietà)
	public int get_deleted() {
		return deleted;
	}
	

}

