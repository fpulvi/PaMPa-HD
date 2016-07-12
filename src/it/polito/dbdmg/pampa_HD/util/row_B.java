package it.polito.dbdmg.pampa_HD.util;

import java.util.List;
import java.util.ArrayList;

public class row_B {
	public row_B (String item, List<Integer> transactions) {
		this.item=item;
		this.transactions=transactions;
	}
	
	public row_B() {};
	
	public String showitem() {
		return item;
	}
	
	public int length() {
		return transactions.size();
	}
	public List<Integer> showlist_trans(){
		return transactions;
	}
	
	//controlla solo la prima riga
	public boolean find_first_and_del(Integer i) {
		boolean found=false;
		int a;
		if (transactions.size()>0){
			a=transactions.get(0);
			//System.out.print("\na="+a);
			if (a==i) {
				found=true;
				transactions.remove(0);
				}
		}
		return found;
		
	}
	
	//funzione per rimuovere un elemento della lista
	public void remove_element(int i){
		transactions.remove((Integer) i);	
	}
	
	public String as_string() {
		String c="";
		for (int b:transactions) c=c+Integer.toString(b)+" ";
		String a= new String (this.item+","+c);
		return a;
	}
	
	public String as_string_spaces() { //not used in B
		String c="";
		for (int b:transactions) c=c+" "+Integer.toString(b)+",";
		String a= new String (this.item+","+c);
		return a;
	}
	
	//metodo clone
	public Object clone() {
		row_B r=new row_B();
	    r.item=this.item;
	    List <Integer> list1= new ArrayList<Integer>();
	    for (Integer i: transactions)
	    {	int a=0;
	    	a=i;
	    	list1.add(a);
	    }
	    r.transactions=list1;
	    return r;
	  }
	
	private String item;
	private List<Integer> transactions;
}

