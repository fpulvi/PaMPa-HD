package it.polito.dbdmg.pampa_HD.util;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;


public class comparator_rid extends WritableComparator {
	
	protected comparator_rid() {
		super(Text.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	public int compare (WritableComparable t1, WritableComparable t2)
	{   
		Text key1kk= (Text) t1;
		Text key2kk= (Text) t2;
		if (key1kk.toString().startsWith("***")) {
			//System.out.println("\nSono dentroooooooooooooooooooo");
			if (key2kk.toString().startsWith("***")) return 1; // qui ci starebbe la comparazione completa
			else return -1; // closed sempre prima per arricchire la memoria
		}
		else if (key2kk.toString().startsWith("***")) {
			//System.out.println("\nSono dentroooooooooooooooooooo2");
			return 1; // qui ci sarebbe la comparazione completa
		 // closed sempre prima per arricchire la memoria
		}
		else {
			//System.out.println("\nSono fuoriiiiii");
		String[] key1k= key1kk.toString().split("\\*");
		String[] key2k= key2kk.toString().split("\\*");
		String key1= key1k[0].trim();
		String key2= key2k[0].trim();
		String[] k1= key1.split(",");
		String[] k2= key2.split(",");
		List<Integer> projection1 = new ArrayList<Integer>();
		List<Integer> projection2 = new ArrayList<Integer>();
		//System.out.println("\nsto per processare: "+key1kk.toString()+" e "+key2kk.toString());
		for (String s: k1) projection1.add(Integer.parseInt(s.trim()));
		for (String s: k2) projection2.add(Integer.parseInt(s.trim()));
		int a=0;
		if (projection1.size()==projection2.size()) {
		//int confronto=Integer.compare(proiezione1.get(a),proiezione2.get(a));
	//	while ((confronto==0)&(k1.length<(a+1))) {
			//a++;
		//	confronto=Integer.compare(proiezione1.get(a),proiezione2.get(a)); }
		int comparison =0; 
		for (int b=0; (b<projection1.size())&(b<projection2.size());b++) {
			comparison=Integer.compare(projection1.get(b),projection2.get(b));
			if (comparison!=0) {
			/*if (confronto==-1) //System.out.println("\nper me "+key1kk.toString()+"viene prima di "+key2kk.toString());
				else if (confronto==1) //System.out.println("\nper me "+key2kk.toString()+"viene prima di "+key1kk.toString());
				if (confronto==0) //System.out.println("\n1 per me "+key2kk.toString()+"è uguale a "+key1kk.toString());	
				
				*/
				return comparison;}
			}
		
		//if (confronto==-1) System.out.println("\nper me "+key1kk.toString()+"viene prima di "+key2kk.toString());
		//else if (confronto==1) System.out.println("\nper me "+key2kk.toString()+"viene prima di "+key1kk.toString());
		//if (confronto==0) System.out.println("\n1 per me "+key2kk.toString()+"è uguale a "+key1kk.toString());
		return comparison;
		}
		else {int comparison=0;
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
			/*
			confronto=Integer.compare(proiezione1.get(proiezione1.size()-2),proiezione2.get(proiezione2.size()-1));
			if (proiezione1.size()>proiezione2.size()) {
				
				if (confronto==0) return -1;
			}
			else confronto=Integer.compare(proiezione1.get(proiezione1.size()-1),proiezione2.get(proiezione2.size()-2));
			if (confronto==-1) System.out.println("\nper me "+key1kk.toString()+"viene prima di "+key2kk.toString());
			else if (confronto==1) System.out.println("\nper me "+key2kk.toString()+"viene prima di "+key1kk.toString());
			else if (confronto==0) System.out.println("\n2 per me "+key2kk.toString()+"è uguale a "+key1kk.toString());
			return confronto;
			*/
		}
		}
		
	}
	

}
