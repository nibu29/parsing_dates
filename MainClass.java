package com.agfa.med.AHDP;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MainClass {
	
	public static String pgdb;
	public static double bs,rec;
	public static int thrds,watchdog;
	public static int[] markerrefs;
	
	public static void main(String[] args) throws SQLException, InterruptedException {
		pgdb = "jdbc:postgresql://localhost:5432/amt?user=amt&password=amt";
		thrds = 4;
		bs=0;
		markerrefs = new int[thrds]; 
		Connection[] cns = new Connection[thrds];
		Statement[] sms = new Statement[thrds];
		ResultSet[] rss = new ResultSet[thrds];
		handle_bodies[] hb = new handle_bodies[thrds];
		
		try {
			Class.forName("org.postgresql.Driver");
			Connection connection = DriverManager.getConnection(pgdb);		
			Statement st = connection.createStatement();
			ResultSet rs = st.executeQuery("select count(*) from hl7_src.report where createddatetime is null");

			if(rs.next()) {
				rs.first();
				rec = rs.getDouble(1);
				bs = Math.ceil(rec / thrds);
			}
			
			if(bs==0) {return;}
			
			markerrefs[0]=0;
			rs = st.executeQuery("select ref from hl7_src.report where createddatetime is null order by ref asc");
			for(int m=1;m<thrds;m++) {
				rs.absolute((int) bs*(m));
				markerrefs[m]=rs.getInt(1);
			}
			rs.last();
			markerrefs[markerrefs.length-1]=rs.getInt(1);
						
			for(int i=0;i<thrds;i++) {
				cns[i]=DriverManager.getConnection(pgdb);
				sms[i]=cns[i].createStatement();
				rss[i]=sms[i].executeQuery("select ref,createddatetime,body from report where createddatetime is null and ref > "+markerrefs[i]+" "
						+ "and ref <="+markerrefs[i+1]);
				hb[i]= new handle_bodies(rss[i]);
			}
			
			rs = st.executeQuery("select count(*) from hl7_src.report where createddatetime is null");
			watchdog = 0;
			int lc=0;
			while(watchdog!=rs.getInt(1)) {
				lc++;
				System.out.println("There are "+rs.getInt(1)+" records with no date.");
				rs = st.executeQuery("select count(*) from hl7_src.report where createddatetime is null");
				watchdog = rs.getInt(1);
				Thread.sleep(60000);
				if(lc>1440) {break;}
			}

			rs.close();
			st.close();
			connection.close();
			for(int i=0;i<thrds;i++) {
				rss[i].close();
				sms[i].close();
				cns[i].close();
			}
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

}
