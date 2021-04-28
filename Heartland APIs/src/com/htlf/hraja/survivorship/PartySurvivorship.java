package com.htlf.hraja.survivorship;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class PartySurvivorship {

	private static Connection con = null;
	private static ResultSet partyFks = null;
	private static ResultSet outputResult1 = null;
	private static int countPartyFks = 0;
	
	public static void main(String[] args) {
		initializeConnection();
		long startTime = System.nanoTime();
		fetchPartyIds();
		long endTime = System.nanoTime();
		long duration = (endTime - startTime); 
		double seconds = (double)duration/1000000000;
		System.out.println("Time taken to fetch Party FKs: "+seconds);
		
		//Rule 0, if there is only one Account for the Party.
		startTime = System.nanoTime();
		processSingleAccountParties();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to process single account parties: "+seconds);
		
		//Execution for the rest of the records which have multiple Accounts for the Party
		executeSurvivorship();
		closeConnection();

	}


	private static void executeSurvivorship() {
		try {
			
			System.out.println("Executing survivorship for the multiple account parties ....");
			partyFks.beforeFirst();
			int count = 0;
			while(partyFks.next()) {
				count++;
				System.out.println(count+" of "+countPartyFks);
				PreparedStatement stmt;
				String sql = " select party.ROWID_OBJECT,party.PKEY_SRC_OBJECT as PARTY_PKEY,party.PRTY_TYP,acc.X_ACC_NMBR,acc.X_ACCOUNT_STATUS,acc.X_SERV_CAT_CD,acc.X_OPEN_DATE,\r\n"
						+ " product.X_PRODUCT_TYPE,\r\n"
						+ " bank.X_LOCATION_CD as banknbr, branch.X_LOCATION_CD as branchnbr,\r\n"
						+ " bank.PKEY_SRC_OBJECT as BANK_PKEY, branch.PKEY_SRC_OBJECT as BRANCH_PKEY, officer.PKEY_SRC_OBJECT as OFFICER_PKEY\r\n"
						+ "  from\r\n"
						+ "C_BO_PRTY_xref party  join\r\n"
						+ "C_XR_PRTY_ACC_REL_xref acc_rel on party.rowid_xref=acc_rel.X_PRTY_FK  join\r\n"
						+ "C_XO_PRTY_ACCOUNT_xref acc on acc.rowid_xref=acc_rel.x_Account_fk \r\n"
						+ "left join C_BO_PRTY_XREF officer on officer.ROWID_XREF = acc.X_BRANCH_REP_FK\r\n"
						+ "left join C_XO_PRODUCT_XREF product\r\n"
						+ "on product.ROWID_XREF = acc.X_PRODUCT_FK\r\n"
						+ "left join C_XO_LOCATION_XREF bank on bank.ROWID_XREF = acc.X_BANK_FK\r\n"
						+ "left join C_XO_LOCATION_XREF branch on branch.ROWID_XREF = acc.X_BRANCH_FK\r\n"
						+ "where party.ROWID_XREF = ? \r\n"
						+ "order by party.ROWID_OBJECT,acc.X_BRANCH_REP_FK";
				stmt = con.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				stmt.setString(1, partyFks.getString("PARTY_XREF"));
				ResultSet rs = stmt.executeQuery();
				
				processSurvivorship(rs);
			}
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		
		
	}


	private static void processSurvivorship(ResultSet rs) throws SQLException {
		
		try {
			
			//Rule 1, if the Officer, Bank and Branch are the same for all records
			
			
			//Rule 2, sort according to serv cat code
			
			
			//Rule 3, sort according to open date
			
			
			//Rule 4, sort according to bank+branch
			
			
			//Rule 5, sort according to the product type
			
			
			//Rule 6, sort according to the Account Number
			
		}
		catch(Exception e) {
			
		}
		
		
		
	}


	private static void fetchPartyIds() {
		System.out.println("Fetching the Party IDs which have more than one Account");
	    try {
			Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			String sql = "select top 3 X_PRTY_FK as PARTY_XREF, count(*) as ACC_COUNT\r\n"
					+ "  from C_XR_PRTY_ACC_REL_xref group by X_PRTY_FK having count(*) > 1 ";
					
			partyFks = stmt.executeQuery(sql);
		    //rs = rs.TYPE_SCROLL_INSENSITIVE;
			partyFks.absolute(2);
		    System.out.println("The 2nd rowid: "+partyFks.getString("PARTY_XREF"));
		    partyFks.last();
		    countPartyFks = partyFks.getRow();
		    System.out.println("Number of Party FKs in Acc_rel Table: "+ countPartyFks);
		    
		} 
	    catch (Exception e) {
			
			e.printStackTrace();
		}
	    
	}
	
	private static void processSingleAccountParties() {
		
		
		Statement stmt;
		try {
			
			stmt = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			String sql = " select party.PKEY_SRC_OBJECT as PARTY_PKEY,officer.PKEY_SRC_OBJECT AS OFFICER_PKEY, bank.PKEY_SRC_OBJECT AS BANK_PKEY, branch.PKEY_SRC_OBJECT AS BRANCH_PKEY\r\n"
					+ "  from\r\n"
					+ "C_BO_PRTY_xref party  join\r\n"
					+ "C_XR_PRTY_ACC_REL_xref acc_rel on party.rowid_xref=acc_rel.X_PRTY_FK  join\r\n"
					+ "C_XO_PRTY_ACCOUNT_xref acc on acc.rowid_xref=acc_rel.x_Account_fk \r\n"
					+ "left join C_BO_PRTY_XREF officer on officer.ROWID_XREF = acc.X_BRANCH_REP_FK\r\n"
					+ "left join C_XO_PRODUCT_XREF product\r\n"
					+ "on product.ROWID_XREF = acc.X_PRODUCT_FK\r\n"
					+ "left join C_XO_LOCATION_XREF bank on bank.ROWID_XREF = acc.X_BANK_FK\r\n"
					+ "left join C_XO_LOCATION_XREF branch on branch.ROWID_XREF = acc.X_BRANCH_FK\r\n"
					+ "where party.ROWID_XREF in \r\n"
					+ "(select  X_PRTY_FK as PARTY_XREF\r\n"
					+ "  from C_XR_PRTY_ACC_REL_xref group by X_PRTY_FK having count(*) = 1)\r\n"
					+ "order by party.ROWID_OBJECT,acc.X_BRANCH_REP_FK";
					
			outputResult1 = stmt.executeQuery(sql);
			outputResult1.last();
			System.out.println("Number of records with Single Accoun: "+ outputResult1.getRow());
		} 
		catch (SQLException e) {
			
			e.printStackTrace();
		}

}

	private static void initializeConnection() {
		
		System.out.println("Connecting to the DS - 2 - MSSQL JDBC Connection");
		
		try {
			String dbURL = "jdbc:sqlserver://10.4.90.81:1433";
			String user = "CUST_ORS";
			String pass = "cust_ors";
			con = DriverManager.getConnection(dbURL, user, pass);
			System.out.println("Connection established");
			
		} 
		catch (Exception e) {
			
			e.printStackTrace();
		}
		
		
	}

	private static void closeConnection() {

		System.out.println("Closing the Conection");
		try {
			con.close();
		} 
		catch (SQLException e) {
			
			e.printStackTrace();
		}
		
	}
}

