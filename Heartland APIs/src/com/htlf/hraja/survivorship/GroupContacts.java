package com.htlf.hraja.survivorship;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class GroupContacts {
	
	private static Connection con = null;

	public static void main(String[] args) {
		
		System.out.println("Running the Process Group Contacts Code\n");
		
		//start time
		long startTotalTime = System.nanoTime();
				
		//Initialize the DB Connection
		initializeConnection();
						
		//Truncate the landing table
		truncateLanding();
		
		//Drop all Temp Tables
		dropTempTables();
		
		//Fetch Head of Household Pairs
		long startTime = System.nanoTime();
		fetchHoHPairs();
		long endTime = System.nanoTime();
		long duration = (endTime - startTime); 
		double seconds = (double)duration/1000000000;
		System.out.println("Time taken to Fetch Head of Household Pairs: "+seconds+" secs.");

		//Process Group Emails
		startTime = System.nanoTime();
		processGroupEmails();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to Process Group Emails: "+seconds+" secs.");	
		
		//Process Group Phones
		startTime = System.nanoTime();
		processGroupPhones();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to Process Group Phones: "+seconds+" secs.");
		

		//Process Group Addresses
		startTime = System.nanoTime();
		processGroupAddresses();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to Process Group Addresses: "+seconds+" secs.");
		
		//Push to Landing
		startTime = System.nanoTime();
		pushToLanding();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to Push to Landing: "+seconds+" secs.");
		
		
		//Drop all Temp Tables
		dropTempTables();
		
		//Closing the Connection
		closeConnection();
		
		//end time
		long endTotalTime = System.nanoTime();
		long toalDuration = (endTotalTime - startTotalTime); 
		double totalSeconds = (double)toalDuration/1000000000;
		System.out.println("Total Time taken for Processing Group Contacts: "+totalSeconds+" secs.");

	}
	
	private static void pushToLanding() {
		System.out.println("\nPushing data to landing");
		try {
			Statement stmt = con.createStatement();
			
			String sql = "insert into C_LDG_GROUP_ADDRESS (GROUP_FK,ADDRESS_FK,ADDR_TYPE,ROWID_OBJECT,MARKETING_PREF)\r\n"
					+ "select PRTY_GRP_FK,PSTL_ADDR_FK,ADDR_TYP,ROWID_OBJECT,X_MRKTNG_PREF\r\n"
					+ "from X_GROUP_CONTACT_ADDRESS_FINAL";
			stmt.execute(sql);
			sql = "insert into C_LDG_GROUP_EMAIL (GROUP_FK,EMAIL_ADDR,EMAIL_TYPE,ROWID_OBJECT,VALIDATION_CODE,STATUS_CD)\r\n"
					+ "select PRTY_GRP_FK,ETRNC_ADDR,ETRNC_ADDR_TYP,ROWID_OBJECT,VLDTN_STS_CD,STS_CD\r\n"
					+ "from X_GROUP_CONTACT_EMAIL_FINAL";
			stmt.execute(sql);
			sql = "insert into C_LDG_GROUP_PHONE (GROUP_FK,PHONE_NUM,PHONE_NUM_EXT,STATUS_CODE,PHONE_TYPE,VALIDATION_CODE,ROWID_OBJECT)\r\n"
					+ "select PRTY_GRP_FK,PHN_NUM,PHN_NUM_EXT,STS_CD,PHN_TYP,VLDTN_STS_CD,ROWID_OBJECT\r\n"
					+ "from X_GROUP_CONTACT_PHONE_FINAL";
			stmt.execute(sql);
			
									
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}
		
	

	private static void fetchHoHPairs() {
		System.out.println("\nFetching the Head of Household Pairs");
		try {
			Statement stmt = con.createStatement();
			
			String sql = "select PRTY_GRP_FK\r\n"
					+ "into X_GROUP_CONTACT_MULTIPLE_PARTIES\r\n"
					+ "from C_BR_PRTY_GRP_REL where REL_TYPE_CODE not like '%Preferred%'  group by PRTY_GRP_FK";
			stmt.execute(sql);
			sql = "select PRTY_FK,PRTY_GRP_FK \r\n"
					+ "into X_GROUP_CONTACT_HEAD_PAIRS\r\n"
					+ "from C_BR_PRTY_GRP_REL where X_HD_HSHLD_FLG = 'Y' and PRTY_GRP_FK in \r\n"
					+ "(select distinct PRTY_GRP_FK from X_GROUP_CONTACT_MULTIPLE_PARTIES)";
			stmt.execute(sql);
			
									
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}
		
	

	private static void processGroupAddresses() {
		System.out.println("\nProcessing the Group Addresses");
		try {
			Statement stmt = con.createStatement();
			
			String sql = "select PRTY_FK,PSTL_ADDR_FK,ADDR_TYP,LAST_UPDATE_DATE,X_MRKTNG_PREF\r\n"
					+ "into X_GROUP_CONTACT_ADDRESS_TEMP\r\n"
					+ "from C_BR_PRTY_RLE_PSTL_ADDR where PRTY_FK in \r\n"
					+ "(select PRTY_FK from X_GROUP_CONTACT_HEAD_PAIRS)";
			stmt.execute(sql);
			sql = "select addr.PRTY_FK,pair.PRTY_GRP_FK,addr.PSTL_ADDR_FK,addr.ADDR_TYP,addr.LAST_UPDATE_DATE,addr.X_MRKTNG_PREF\r\n"
					+ "into X_GROUP_CONTACT_ADDRESS\r\n"
					+ "from X_GROUP_CONTACT_ADDRESS_TEMP addr\r\n"
					+ "join X_GROUP_CONTACT_HEAD_PAIRS pair\r\n"
					+ "on addr.PRTY_FK = pair.PRTY_FK";
			stmt.execute(sql);
			sql = "select addr.PRTY_GRP_FK,addr.PSTL_ADDR_FK,addr.ADDR_TYP,addr.LAST_UPDATE_DATE,pstl.ROWID_OBJECT,addr.X_MRKTNG_PREF\r\n"
					+ "into X_GROUP_CONTACT_ADDRESS_TEMP1\r\n"
					+ "from X_GROUP_CONTACT_ADDRESS addr\r\n"
					+ "join C_BR_PRTY_RLE_PSTL_ADDR pstl\r\n"
					+ "on addr.PRTY_GRP_FK = pstl.X_GROUP_FK";
			stmt.execute(sql);
			sql = "select top 1 with ties\r\n"
					+ "PRTY_GRP_FK,PSTL_ADDR_FK,ADDR_TYP,ROWID_OBJECT,X_MRKTNG_PREF\r\n"
					+ "into X_GROUP_CONTACT_ADDRESS_FINAL\r\n"
					+ "from X_GROUP_CONTACT_ADDRESS_TEMP1\r\n"
					+ "order by row_number() over (partition by PRTY_GRP_FK\r\n"
					+ "order by\r\n"
					+ "case when ADDR_TYP = 'Home' then 0 else 1 end,\r\n"
					+ "LAST_UPDATE_DATE desc)";
			stmt.execute(sql);
			
									
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}

	private static void processGroupPhones() {
		System.out.println("\nProcessing the Group Phones");
		try {
			Statement stmt = con.createStatement();
			
			String sql = "select PRTY_FK,PHN_NUM,PHN_NUM_EXT,STS_CD,PHN_TYP,LAST_UPDATE_DATE,VLDTN_STS_CD\r\n"
					+ "into X_GROUP_CONTACT_PHONE_TEMP\r\n"
					+ "from C_BO_PRTY_RLE_PHN_COMM where PRTY_FK in\r\n"
					+ "(select PRTY_FK from X_GROUP_CONTACT_HEAD_PAIRS)";
			stmt.execute(sql);
			sql = "select ph.PRTY_FK,pair.PRTY_GRP_FK,ph.PHN_NUM,ph.PHN_NUM_EXT,ph.STS_CD,ph.PHN_TYP,ph.LAST_UPDATE_DATE,ph.VLDTN_STS_CD\r\n"
					+ "into X_GROUP_CONTACT_PHONE_TEMP1\r\n"
					+ "from X_GROUP_CONTACT_PHONE_TEMP ph\r\n"
					+ "join X_GROUP_CONTACT_HEAD_PAIRS pair\r\n"
					+ "on ph.PRTY_FK = pair.PRTY_FK";
			stmt.execute(sql);
			sql = "select tmp.PRTY_GRP_FK,tmp.PHN_NUM,tmp.PHN_NUM_EXT,tmp.STS_CD,tmp.PHN_TYP,tmp.LAST_UPDATE_DATE,ph.ROWID_OBJECT,tmp.VLDTN_STS_CD\r\n"
					+ "into X_GROUP_CONTACT_PHONE_TEMP2\r\n"
					+ "from X_GROUP_CONTACT_PHONE_TEMP1 tmp\r\n"
					+ "join C_BO_PRTY_RLE_PHN_COMM ph\r\n"
					+ "on tmp.PRTY_GRP_FK = ph.X_GROUP_FK";
			stmt.execute(sql);
			sql = "select top 1 with ties\r\n"
					+ "PRTY_GRP_FK,PHN_NUM,PHN_NUM_EXT,STS_CD,PHN_TYP,ROWID_OBJECT,VLDTN_STS_CD\r\n"
					+ "into X_GROUP_CONTACT_PHONE_FINAL\r\n"
					+ "from X_GROUP_CONTACT_PHONE_TEMP2\r\n"
					+ "order by row_number() over (partition by PRTY_GRP_FK\r\n"
					+ "order by\r\n"
					+ "case when PHN_TYP = 'Home' then 0 else 1 end,\r\n"
					+ "LAST_UPDATE_DATE desc)";
			stmt.execute(sql);
			
									
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}
		
	

	private static void processGroupEmails() {
		System.out.println("\nProcessing the Group Emails");
		try {
			Statement stmt = con.createStatement();
			
			String sql = "select PRTY_FK,ETRNC_ADDR,ETRNC_ADDR_TYP,STS_CD,LAST_UPDATE_DATE,VLDTN_STS_CD\r\n"
					+ "into X_GROUP_CONTACT_EMAIL_TEMP\r\n"
					+ "from C_BO_PRTY_RLE_ETRNC_ADDR where PRTY_FK in \r\n"
					+ "(select PRTY_FK from X_GROUP_CONTACT_HEAD_PAIRS)";
			stmt.execute(sql);
			sql = "select email.PRTY_FK,pair.PRTY_GRP_FK,email.ETRNC_ADDR,email.STS_CD,email.ETRNC_ADDR_TYP,email.LAST_UPDATE_DATE,email.VLDTN_STS_CD\r\n"
					+ "into X_GROUP_CONTACT_EMAIL_TEMP1\r\n"
					+ "from X_GROUP_CONTACT_EMAIL_TEMP email\r\n"
					+ "join X_GROUP_CONTACT_HEAD_PAIRS pair\r\n"
					+ "on email.PRTY_FK = pair.PRTY_FK";
			stmt.execute(sql);
			sql = "select tmp.PRTY_GRP_FK, tmp.ETRNC_ADDR, tmp.STS_CD, tmp.ETRNC_ADDR_TYP, tmp.LAST_UPDATE_DATE, email.ROWID_OBJECT, tmp.VLDTN_STS_CD\r\n"
					+ "into X_GROUP_CONTACT_EMAIL_TEMP2\r\n"
					+ "from X_GROUP_CONTACT_EMAIL_TEMP1 tmp\r\n"
					+ "join C_BO_PRTY_RLE_ETRNC_ADDR email\r\n"
					+ "on tmp.PRTY_GRP_FK = email.X_GROUP_FK";
			stmt.execute(sql);
			sql = "select top 1 with ties\r\n"
					+ "PRTY_GRP_FK, ETRNC_ADDR, STS_CD, ETRNC_ADDR_TYP, LAST_UPDATE_DATE, ROWID_OBJECT, VLDTN_STS_CD\r\n"
					+ "into X_GROUP_CONTACT_EMAIL_FINAL\r\n"
					+ "from X_GROUP_CONTACT_EMAIL_TEMP2\r\n"
					+ "order by row_number() over (partition by PRTY_GRP_FK\r\n"
					+ "order by\r\n"
					+ "case when ETRNC_ADDR_TYP = 'EMAIL-Home' then 0 else 1 end,\r\n"
					+ "LAST_UPDATE_DATE desc)";
			stmt.execute(sql);
			
									
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}
		
	

	private static void initializeConnection() {
		
		System.out.println("Connecting to the DS - 2 - MSSQL JDBC Connection");
		
		try {
			
			FileReader fr = new FileReader("db.properties");
			
			Properties properties = new Properties();
			properties.load(fr);
			
			//System.out.println("URL from the properties file: "+properties.getProperty("database.url"));
			
			String dbURL = properties.getProperty("database.url");
			String user = properties.getProperty("database.username");
			String pass = properties.getProperty("database.password");
			con = DriverManager.getConnection(dbURL, user, pass);
			System.out.println("Connection established");
			
		} 
		catch (Exception e) {
			
			e.printStackTrace();
		}
		
		
	}
	
	private static void closeConnection() {

		System.out.println("\nClosing the Conection");
		try {
			con.close();
		} 
		catch (SQLException e) {
			
			e.printStackTrace();
		}
		
	}

	private static void truncateLanding() {
		System.out.println("\nTruncating the Landing Tables");
		try {
			Statement stmt = con.createStatement();
			String sql = "truncate table C_LDG_GROUP_ADDRESS";
			stmt.execute(sql);
			sql = "truncate table C_LDG_GROUP_EMAIL";
			stmt.execute(sql);
			sql = "truncate table C_LDG_GROUP_PHONE";
			stmt.execute(sql);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}

	private static void dropTempTables() {
		System.out.println("\nDropping all temp tables");
		try {
			Statement stmt = con.createStatement();
			String sql = "DROP TABLE IF EXISTS X_GROUP_CONTACT_MULTIPLE_PARTIES";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_CONTACT_HEAD_PAIRS";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_CONTACT_ADDRESS_TEMP";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_CONTACT_ADDRESS";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_CONTACT_ADDRESS_TEMP1";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_CONTACT_ADDRESS_FINAL";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_CONTACT_PHONE_TEMP";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_CONTACT_PHONE_TEMP1";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_CONTACT_PHONE_TEMP2";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_CONTACT_PHONE_FINAL";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_CONTACT_EMAIL_TEMP";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_CONTACT_EMAIL_TEMP1";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_CONTACT_EMAIL_TEMP2";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_CONTACT_EMAIL_FINAL";
			stmt.execute(sql);
						
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}

}
