package com.agfa.med.AHDP;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class handle_bodies {
	
	public handle_bodies(ResultSet rs) {
		class NuThread implements Runnable {
			ResultSet rs;
			handle_bodies hb;
			NuThread(ResultSet rs,handle_bodies hb) {
				this.rs=rs;
				this.hb=hb;
			}
			@Override
			public void run() {
				try {
					rs.beforeFirst();
					while(rs.next()) {
						String body = rs.getString(3);
						String[] potential_dates = hb.begin_parse(body);
						String gooddate = hb.form_dates(potential_dates);
						rs.updateString(2, gooddate);
						rs.updateRow();
					}
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
			}			
		}
		Thread t = new Thread(new NuThread(rs,this));
		t.start();
	}
	
	public String[] begin_parse(String b) {
		Pattern p1 = Pattern.compile("\\d{1,4}[.]\\D{3,9}[.]\\d{2,4}");
		Pattern p2 = Pattern.compile("\\d{1,4}[.]\\d{2}[.]\\d{2,4}");
		Pattern p3 = Pattern.compile("\\D{3,9}\\s\\d{1,2}[,]\\s\\d{2,4}");
		Matcher m1,m2,m3;
		ArrayList<String> dates = new ArrayList<String>();
		Scanner sc1 = new Scanner(b);
		
		if(!sc1.hasNextLine()) {
			sc1.close();
			return new String[0];
		}
		
		while(sc1.hasNextLine()) {
			String templine = sc1.nextLine();
			int d = templine.toUpperCase().indexOf("DATE");
			if(d>0) {
				m1=p1.matcher(templine);
				while(m1.find()) {
					dates.add(templine.substring(m1.start(), m1.end()-1));
				}
				m2=p2.matcher(templine);
				while(m2.find()) {
					dates.add(templine.substring(m2.start(),m2.end()-1));
				}
				m3=p3.matcher(templine);
				while(m3.find()) {
					dates.add(templine.substring(m3.start(),m3.end()-1));
				}
			}			
		}
		sc1.close();
		if(dates.size()>0) {
			dates.trimToSize();
			return dates.toArray(new String[dates.size()]);
		} 
		else {
			return new String[0];
		}
		
	}
	
	public String form_dates(String[] s) {
		ArrayList<String> cleandates=new ArrayList<>();
		Pattern py1 = Pattern.compile("\\d{4}");
		Pattern pm1 = Pattern.compile("[A-Za-z].*[A-Za-z]");
		Pattern pd1 = Pattern.compile("\\d{1,2}[-]");
		Pattern py2 = Pattern.compile("[-]\\d{2}");
		Pattern pd2 = Pattern.compile("\\s\\d{1,2}[,]\\s");
		Pattern pmdy = Pattern.compile("\\d{1,2}[/]\\d{1,2}[/]\\d{2}");
		Matcher my1,mm1,md1,my2,md2,mmdy;
		for(String str:s) {
			String year="00",month="00",day="00";
			
			my1=py1.matcher(str);
			while(my1.find()) {
				year=str.substring(my1.start(), my1.end()-1);
			}
			
			mm1=pm1.matcher(str);
			while(mm1.find()) {
				if(month.length()==2&&month.contains("00")) {
					month=this.month_lookup(str.substring(mm1.start(), mm1.end()-1));
				}
			}
			
			md1=pd1.matcher(str);
			while(md1.find()) {
				day="00"+str.substring(md1.start(), md1.end()-2);
				day=day.substring(day.length()-2, day.length());
			}
			
			if(year.length()==2 && year.contains("00")) {
				my2=py2.matcher(str);
				while(my2.find()) {
					year=str.substring(my2.start()+1, my2.end()-1);
					if(Integer.parseInt(year)<19) {
						year = "20"+year;
					} else {
						year = "19"+year;
					}
				}
			}
			
			if(day.length()==2&&day.contains("00")) {
				md2=pd2.matcher(str);
				while(md2.find()) {
					day = "00"+str.substring(md2.start()+1,md2.end()-3);
					day=day.substring(day.length()-2, day.length());
				}
			}
			
			if((day.length()==2&&day.contains("00"))||(month.length()==2&&month.contains("00"))||(year.length()==2&&year.contains("00"))) {
				mmdy=pmdy.matcher(str);
				while(mmdy.find()) {
					String[] buff = str.split("/");
					if(Integer.parseInt(buff[0])>12) {
						if(day.length()==2&&day.contains("00")) {day = buff[0];}
						if(month.length()==2&&month.contains("00")) {month = buff[1];}
					} else {
						if(day.length()==2&&day.contains("00")) {day = buff[1];}
						if(month.length()==2&&month.contains("00")) {month = buff[0];}
					}
					
					if(year.length()==2&&year.contains("00")) {
						if(Integer.parseInt(buff[2])<19) {
							year = "20"+buff[2];
						} else {
							year = "19"+buff[2];
						}
					}
						
				}
			}
			cleandates.add(year+month+day);
		}
		if (cleandates.size()>0) {
			cleandates.trimToSize();
			Collections.sort(cleandates,Collections.reverseOrder());
			return cleandates.get(0);
		} else {
			return null;
		}
		
	}
	
	public String month_lookup(String s) {
		String month = s.toUpperCase();
		if(month.contains("JAN")) {
			return "01";
		} else
		if(month.contains("FEB")) {
			return "02";
		} else
		if(month.contains("MAR")) {
			return "03";
		} else
		if(month.contains("APR")) {
			return "04";
		}
		if(month.contains("MAY")) {
			return "05";
		} else
		if(month.contains("JUN")) {
			return "06";
		} else
		if(month.contains("JUL")) {
			return "07";
		} else
		if(month.contains("AUG")) {
			return "08";
		} else
		if(month.contains("SEP")) {
			return "09";
		} else
		if(month.contains("OCT")) {
			return "10";
		} else
		if(month.contains("NOV")) {
			return "11";
		} else
		if(month.contains("DEC")) {
			return "12";
		} else {
			return "00";
		}
			
	}
}


