package com.htlf.hraja.survivorship;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class HouseholdSurvivorship {
	
	private static Connection con = null;

	public static void main(String[] args) {
		
		System.out.println("Running the Household Survivorship Code\n");
		
		//start time
		long startTotalTime = System.nanoTime();
		
		//Initialize the DB Connection
		initializeConnection();
				
		//Truncate the landing table
		truncateLanding();
		
		//Drop all Temp Tables
		dropTempTables();
		
		//Create the main Temp Table
		long startTime = System.nanoTime();
		createMainTempTable();
		long endTime = System.nanoTime();
		long duration = (endTime - startTime); 
		double seconds = (double)duration/1000000000;
		System.out.println("Time taken to create main temp table: "+seconds+" secs.");
		
		//Rule 0, Populate the group with single parties in the temp landing table
		startTime = System.nanoTime();
		processSinglePartygroups();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to process single party groups: "+seconds+" secs.");
		
		//Rule 1, Order each party group by party type, only the highest party tpe should survive (1.Org,2.Person)
		startTime = System.nanoTime();
		processRule1();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to process Rule 1: "+seconds+" secs.");
		
		//Rule 2, Only the party which has the highest number of accounts from each party group should survive
		startTime = System.nanoTime();
		processRule2();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to process Rule 2: "+seconds+" secs.");
		
		//Rule 3, sort according to first cntct date(Asc,date of incorporation) and then birth date(Asc)
		startTime = System.nanoTime();
		processRule3();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to process Rule 3: "+seconds+" secs.");
		
		//Populate the landing table
		startTime = System.nanoTime();
		populateLanding();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to populate landing table: "+seconds+" secs.");
		
		//Modify the landing table
		startTime = System.nanoTime();
		modifyLanding();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to modify the landing table: "+seconds+" secs.");
		
		//Drop all Temp Tables
		dropTempTables();
		
		//Closing the Connection
		closeConnection();
		
		//end time
		long endTotalTime = System.nanoTime();
		long toalDuration = (endTotalTime - startTotalTime); 
		double totalSeconds = (double)toalDuration/1000000000;
		System.out.println("Total Time taken for Household survivorship: "+totalSeconds+" secs.");

	}
	
	private static void modifyLanding() {
		System.out.println("\nModifying the Landing table");
		try {
			Statement stmt = con.createStatement();
			
			String sql = "insert into C_HOUSEHOLD_SURVIVORSHIP (GROUP_PK,PARTY_PK,PRTY_TYP,OFFICER_PK,BANK_PK,BRANCH_PK,LOCATION_TYP,GROUP_BANKING_ROWID,GROUP_REL_ROWID)\r\n"
					+ "select PRTY_GRP_FK,PRTY_FK,\r\n"
					+ "case when REL_TYPE_CODE = 'Belongs to' then 'Person'\r\n"
					+ "when REL_TYPE_CODE = 'Belong to Comm' then 'Org'\r\n"
					+ "end as Prty_typ,null,null,null, 'HoH_N' as location_typ, null,ROWID_OBJECT as grp_rel_rowid\r\n"
					+ "from C_BR_PRTY_GRP_REL where X_HD_HSHLD_FLG = 'Y' and LAST_ROWID_SYSTEM = 'CUSTOM' and concat(PRTY_GRP_FK,PRTY_FK) in\r\n"
					+ "(select concat(PRTY_GRP_FK,PRTY_FK) from C_BR_PRTY_GRP_REL where X_HD_HSHLD_FLG = 'Y' and HUB_STATE_IND = 1 and LAST_ROWID_SYSTEM = 'CUSTOM' and concat(PRTY_GRP_FK,PRTY_FK) not in \r\n"
					+ "(select concat(PRTY_GRP_FK,PRTY_FK) from C_BR_PRTY_GRP_REL where X_HD_HSHLD_FLG = 'Y' and HUB_STATE_IND = 1 and LAST_ROWID_SYSTEM = 'CUSTOM' and concat(PRTY_GRP_FK,PRTY_FK)  in \r\n"
					+ "(select concat(GROUP_PK,PARTY_PK) from C_HOUSEHOLD_SURVIVORSHIP where LOCATION_TYP = 'HoH')))";
			stmt.execute(sql);
			sql = "delete from C_HOUSEHOLD_SURVIVORSHIP where LOCATION_TYP = 'HoH' and GROUP_PK  in \r\n"
					+ "(select PRTY_GRP_FK from C_BR_PRTY_GRP_REL where X_HD_HSHLD_FLG = 'Y' and LAST_ROWID_SYSTEM = 'SYS0' and HUB_STATE_IND = 1)";
			stmt.execute(sql);
			
									
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}

	private static void populateLanding() {
		System.out.println("\nPopulating the Landing table");
		try {
			Statement stmt = con.createStatement();
			
			String sql = "insert into C_HOUSEHOLD_SURVIVORSHIP (GROUP_PK,PARTY_PK,PRTY_TYP,OFFICER_PK,BANK_PK,BRANCH_PK,LOCATION_TYP,GROUP_BANKING_ROWID,GROUP_REL_ROWID) \r\n"
					+ "select distinct grp_temp.GROUP_PK,grp_temp.PARTY_PK, \r\n"
					+ "prty_ldg.PRTY_TYP,prty_ldg.OFFICER_PK,prty_ldg.BANK_PK,prty_ldg.BRANCH_PK,prty_ldg.LOCATION_TYP,\r\n"
					+ "grp_bnk.ROWID_OBJECT as GROUP_BANKING_ROWID, null as GROUP_REL_ROWID\r\n"
					+ "from X_HOUSEHOLD_SURVIVORSHIP_TEMP grp_temp\r\n"
					+ "join C_PARTY_SURVIVORSHIP prty_ldg\r\n"
					+ "on grp_temp.PARTY_PK = prty_ldg.PARTY_PK\r\n"
					+ "join C_XR_PRTY_GRP_BNKNG_REL grp_bnk \r\n"
					+ "on grp_bnk.X_GROUP_FK = grp_temp.GROUP_PK and \r\n"
					+ "prty_ldg.LOCATION_TYP = 'Branch' and grp_bnk.X_REL_TYPE_CODE like '%Branch%' and grp_bnk.HUB_STATE_IND = '1'";
			stmt.execute(sql);
			sql = "insert into C_HOUSEHOLD_SURVIVORSHIP (GROUP_PK,PARTY_PK,PRTY_TYP,OFFICER_PK,BANK_PK,BRANCH_PK,LOCATION_TYP,GROUP_BANKING_ROWID,GROUP_REL_ROWID) \r\n"
					+ "select distinct grp_temp.GROUP_PK,grp_temp.PARTY_PK, \r\n"
					+ "prty_ldg.PRTY_TYP,prty_ldg.OFFICER_PK,prty_ldg.BANK_PK,prty_ldg.BRANCH_PK,prty_ldg.LOCATION_TYP,\r\n"
					+ "grp_bnk.ROWID_OBJECT as GROUP_BANKING_ROWID, null as GROUP_REL_ROWID\r\n"
					+ "from X_HOUSEHOLD_SURVIVORSHIP_TEMP grp_temp\r\n"
					+ "join C_PARTY_SURVIVORSHIP prty_ldg\r\n"
					+ "on grp_temp.PARTY_PK = prty_ldg.PARTY_PK\r\n"
					+ "join C_XR_PRTY_GRP_BNKNG_REL grp_bnk \r\n"
					+ "on grp_bnk.X_GROUP_FK = grp_temp.GROUP_PK and \r\n"
					+ "prty_ldg.LOCATION_TYP = 'Bank' and grp_bnk.X_REL_TYPE_CODE like '%Bank%' and grp_bnk.HUB_STATE_IND = '1'";
			stmt.execute(sql);
			sql = "insert into C_HOUSEHOLD_SURVIVORSHIP (GROUP_PK,PARTY_PK,PRTY_TYP,OFFICER_PK,BANK_PK,BRANCH_PK,LOCATION_TYP,GROUP_BANKING_ROWID,GROUP_REL_ROWID) \r\n"
					+ "select distinct grp_temp.GROUP_PK,grp_temp.PARTY_PK, \r\n"
					+ "prty_ldg.PRTY_TYP,prty_ldg.OFFICER_PK,prty_ldg.BANK_PK,prty_ldg.BRANCH_PK,prty_ldg.LOCATION_TYP,\r\n"
					+ "null as GROUP_BANKING_ROWID, grp_rel.ROWID_OBJECT as GROUP_REL_ROWID\r\n"
					+ "from X_HOUSEHOLD_SURVIVORSHIP_TEMP grp_temp\r\n"
					+ "join C_PARTY_SURVIVORSHIP prty_ldg\r\n"
					+ "on grp_temp.PARTY_PK = prty_ldg.PARTY_PK\r\n"
					+ "join C_BR_PRTY_GRP_REL grp_rel\r\n"
					+ "on grp_temp.GROUP_PK = grp_rel.PRTY_GRP_FK and grp_rel.REL_TYPE_CODE in ('Preferred Officer') and grp_rel.HUB_STATE_IND = '1'\r\n"
					+ "and prty_ldg.LOCATION_TYP = 'Officer'";
			stmt.execute(sql);
			
			
			sql = "insert into C_HOUSEHOLD_SURVIVORSHIP (GROUP_PK,PARTY_PK,PRTY_TYP,OFFICER_PK,BANK_PK,BRANCH_PK,LOCATION_TYP,GROUP_BANKING_ROWID,GROUP_REL_ROWID) \r\n"
					+ "select distinct grp_temp.GROUP_PK,grp_temp.PARTY_PK, \r\n"
					+ "grp_temp.PRTY_TYP,null as OFFICER_PK,null as BANK_PK,null as BRANCH_PK,'HoH' as LOCATION_TYP,\r\n"
					+ "null as GROUP_BANKING_ROWID, grp_rel.ROWID_OBJECT as GROUP_REL_ROWID\r\n"
					+ "from X_HOUSEHOLD_SURVIVORSHIP_TEMP grp_temp\r\n"
					+ "join C_BR_PRTY_GRP_REL grp_rel\r\n"
					+ "on grp_temp.GROUP_PK = grp_rel.PRTY_GRP_FK and grp_rel.REL_TYPE_CODE not in ('Preferred Officer') and grp_rel.HUB_STATE_IND = '1'\r\n"
					+ "and grp_temp.party_pk = grp_rel.PRTY_FK";
			stmt.execute(sql);
			
			sql = "DROP TABLE IF EXISTS X_HOUSEHOLD_SURVIVORSHIP_TEMP";
			stmt.execute(sql);
									
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}

	private static void processRule3() {
		System.out.println("\nProcessing the Rule 3");
		try {
			Statement stmt = con.createStatement();
			String sql = "insert into X_HOUSEHOLD_SURVIVORSHIP_TEMP (PARTY_PK,GROUP_PK,PRTY_TYP)\r\n"
					+ "select top 1 with ties\r\n"
					+ "PARTY_PK,GROUP_PK,PRTY_TYP\r\n"
					+ "from X_GROUP_SURVIVORSHIP_MAIN\r\n"
					+ "order by row_number() over (partition by GROUP_PK \r\n"
					+ "order by \r\n"
					+ "case when X_ACCOUNT_FK is null then 1 else 0 end,\r\n"
					+ "case when DT_OF_INCRPRTN is null then 1 else 0 end, DT_OF_INCRPRTN,\r\n"
					+ "case when BRTH_DT is null then 1 else 0 end, BRTH_DT)";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_SURVIVORSHIP_MAIN";
			stmt.execute(sql);						
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}

	private static void processRule2() {
		System.out.println("\nProcessing the Rule 2");
		try {
			Statement stmt = con.createStatement();
			String sql = "select GROUP_PK,PARTY_PK,count(*) as Account_Count\r\n"
					+ "into X_GROUP_SURV_RULE2\r\n"
					+ " from X_GROUP_SURVIVORSHIP_MAIN group by GROUP_PK,PARTY_PK";
			stmt.execute(sql);
			sql = "  delete from X_GROUP_SURVIVORSHIP_MAIN where concat(GROUP_PK,PARTY_PK) not in(\r\n"
					+ " select concat(GROUP_PK,PARTY_PK) from X_GROUP_SURV_RULE2 where concat(GROUP_PK,'|',Account_Count) in\r\n"
					+ " (select concat(GROUP_PK,'|',MAX(Account_Count)) from X_GROUP_SURV_RULE2 group by GROUP_PK))";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_SURV_RULE2";
			stmt.execute(sql);
						
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}

	private static void processRule1() {
		System.out.println("\nProcessing the Rule 1");
		try {
			Statement stmt = con.createStatement();
			String sql = "select distinct GROUP_PK, PARTY_PK, PRTY_TYP\r\n"
					+ "into X_GROUP_SURV_RULE1\r\n"
					+ "from X_GROUP_SURVIVORSHIP_MAIN";
			stmt.execute(sql);
			sql = " delete from X_GROUP_SURVIVORSHIP_MAIN where concat(GROUP_PK,'|',PRTY_TYP) not in \r\n"
					+ " (select concat(GROUP_PK,'|',MIN(PRTY_TYP)) as PRTY_TYPE from X_GROUP_SURV_RULE1 group by GROUP_PK)";
			stmt.execute(sql);
			sql = " DROP TABLE IF EXISTS X_GROUP_SURV_RULE1";
			stmt.execute(sql);
						
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}

	private static void processSinglePartygroups() {
		System.out.println("\nProcessing the Single Party Groups");
		try {
			Statement stmt = con.createStatement();
			String sql = "select distinct GROUP_PK, PARTY_PK\r\n"
					+ "into X_GROUP_SURV_RULE0\r\n"
					+ "from X_GROUP_SURVIVORSHIP_MAIN";
			stmt.execute(sql);
			sql = "select distinct GROUP_PK, PARTY_PK, PRTY_TYP\r\n"
					+ "into X_HOUSEHOLD_SURVIVORSHIP_TEMP\r\n"
					+ "from X_GROUP_SURVIVORSHIP_MAIN where GROUP_PK in \r\n"
					+ "(select GROUP_PK from X_GROUP_SURV_RULE0 group by GROUP_PK having count(*) = 1)";
			stmt.execute(sql);
			sql = "delete from X_GROUP_SURVIVORSHIP_MAIN where GROUP_PK in\r\n"
					+ "(select GROUP_PK from X_GROUP_SURV_RULE0 group by GROUP_PK having count(*) = 1)";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_SURV_RULE0";
			stmt.execute(sql);
			
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}

	private static void createMainTempTable() {
		System.out.println("\nCreating the main Temp Table");
		try {
			Statement stmt = con.createStatement();
			String sql = "select distinct grp.ROWID_OBJECT as GROUP_PK,\r\n"
					+ "prty.ROWID_OBJECT as PARTY_PK,prty.PRTY_TYP, prty.BRTH_DT,prty.DT_OF_INCRPRTN,\r\n"
					+ "acc_rel.X_ACCOUNT_FK\r\n"
					+ "into X_GROUP_SURVIVORSHIP_MAIN\r\n"
					+ "from C_BO_PRTY_GRP_XREF grp\r\n"
					+ "join C_BR_PRTY_GRP_REL_XREF grp_rel\r\n"
					+ "on grp.ROWID_XREF = grp_rel.PRTY_GRP_FK and grp_rel.REL_TYPE_CODE in ('Belongs to','Belong to Comm') and grp_rel.HUB_STATE_IND = '1'\r\n"
					+ "join C_BO_PRTY_XREF prty\r\n"
					+ "on prty.ROWID_XREF = grp_rel.PRTY_FK and prty.HUB_STATE_IND = '1'\r\n"
					+ "left join C_XR_PRTY_ACC_REL_XREF acc_rel \r\n"
					+ "on prty.rowid_xref=acc_rel.X_PRTY_FK  and acc_rel.HUB_STATE_IND = '1'\r\n"
					+ "where grp.HUB_STATE_IND = '1'\r\n"
					+ "order by grp.ROWID_OBJECT,prty.BRTH_DT";
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
		System.out.println("\nTruncating the Household Survivorship Landing Table");
		try {
			Statement stmt = con.createStatement();
			String sql = "truncate table C_HOUSEHOLD_SURVIVORSHIP";
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
			String sql = "DROP TABLE IF EXISTS X_GROUP_SURVIVORSHIP_MAIN";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_SURV_RULE0";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_HOUSEHOLD_SURVIVORSHIP_TEMP";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_SURV_RULE1";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_GROUP_SURV_RULE2";
			stmt.execute(sql);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}

}