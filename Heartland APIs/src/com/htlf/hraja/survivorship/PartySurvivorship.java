package com.htlf.hraja.survivorship;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class PartySurvivorship {
	
	private static Connection con = null;

	
	public static void main(String[] args) {

		System.out.println("Running the Party Survivorship Code\n");
		
		//start time
		long startTotalTime = System.nanoTime();
		
		
		//Initialize the DB Connection
		initializeConnection();
		
		//Truncate the landing table
		truncateLanding();
		
		//Drop all Temp Tables
		dropTempTables();
				
		//Rule 0, if there is only one Account for the Party.
		long startTime = System.nanoTime();
		processSingleAccountParties();
		long endTime = System.nanoTime();
		long duration = (endTime - startTime); 
		double seconds = (double)duration/1000000000;
		System.out.println("Time taken to process single account parties: "+seconds+" secs.");
		
		
		//Create the main Temp Table
		startTime = System.nanoTime();
		createMainTempTable();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to create main temp table: "+seconds+" secs.");
		
		//Remove Participation loan = P records
		startTime = System.nanoTime();
		processParticipationLoan();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to process participation loan records: "+seconds+" secs.");
		
		//Populate the priority for the serv_cat_code
		startTime = System.nanoTime();
		populatePriority();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to update the priorities: "+seconds+" secs.");
		
		//Rule 1, 
		startTime = System.nanoTime();
		proccessRule1();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to process Rule 1: "+seconds+" secs.");
		
		//Rule 2,
		startTime = System.nanoTime();
		proccessRule2();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to process Rule 2: "+seconds+" secs.");
		
		//Rule 3,
		startTime = System.nanoTime();
		proccessRule3();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to process Rule 3: "+seconds+" secs.");
				
		//Rule 4,
		startTime = System.nanoTime();
		proccessRule4();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to process Rule 4: "+seconds+" secs.");	
		
		//Remaining rules,
		startTime = System.nanoTime();
		proccessRemainingRules();
		endTime = System.nanoTime();
		duration = (endTime - startTime); 
		seconds = (double)duration/1000000000;
		System.out.println("Time taken to process remaining rules 4: "+seconds+" secs.");	
		
		//Drop all Temp Tables
		dropTempTables();
		
		closeConnection();
		
		//end time
		long endTotalTime = System.nanoTime();
		long toalDuration = (endTotalTime - startTotalTime); 
		double totalSeconds = (double)toalDuration/1000000000;
		System.out.println("Total Time taken for Party survivorship: "+totalSeconds+" secs.");

	}


	private static void processParticipationLoan() {
		System.out.println("\nProcessing participation loan records");
		try {
			Statement stmt = con.createStatement();
			String sql = "select top 1 with ties\r\n"
					+ "PARTY_PKEY,x_loan_participation\r\n"
					+ "into X_PARTY_SURV_PART_LOAN\r\n"
					+ "from X_PARTY_SURV_MAIN\r\n"
					+ "order by row_number() over (partition by PARTY_PKEY\r\n"
					+ "order by\r\n"
					+ "case when x_loan_participation = 'P' then 1 else 0 end)";
			stmt.execute(sql);
			sql = "delete from X_PARTY_SURV_MAIN where x_loan_participation='P' and PARTY_PKEY not in \r\n"
					+ "(select party_pkey from X_PARTY_SURV_PART_LOAN where x_loan_participation = 'P')";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_PARTY_SURV_PART_LOAN";
			stmt.execute(sql);
			
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}


	private static void proccessRemainingRules() {
		System.out.println("\nProcessing Remaining Rules");
		try {
			Statement stmt = con.createStatement();
			String sql = "insert into X_PARTY_SURVIVORSHIP_TEMP (PARTY_PK,OFFICER_PK,BANK_PK,BRANCH_PK,PRTY_TYP)\r\n"
					+ "select top 1 with ties\r\n"
					+ "PARTY_PKEY,OFFICER_PKEY,BANK_PKEY,BRANCH_PKEY,PRTY_TYP   \r\n"
					+ "from X_PARTY_SURV_MAIN\r\n"
					+ "order by row_number() over (partition by PARTY_PKEY \r\n"
					+ "order by PARTY_PKEY,\r\n"
					+ "case when concat(banknbr,branchnbr) is null then 1 else 0 end, concat(banknbr,branchnbr),\r\n"
					+ "case when X_PRODUCT_TYPE is null then 1 else 0 end, X_PRODUCT_TYPE,\r\n"
					+ "case when X_ACC_NMBR is null then 1 else 0 end, X_ACC_NMBR)";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_PARTY_SURV_MAIN";
			stmt.execute(sql);
			
			System.out.println("\nPushing Data to the Landing Table");
			
			sql = "select PARTY_PK,OFFICER_PK,null as BANK_PK,BRANCH_PK,PRTY_TYP,'Branch' as LOCATION_TYP \r\n"
					+ "into X_PRE_PARTY_SURVIVORSHIP\r\n"
					+ "from X_PARTY_SURVIVORSHIP_TEMP";
			stmt.execute(sql);
			sql = "insert into X_PRE_PARTY_SURVIVORSHIP (PARTY_PK,OFFICER_PK,BANK_PK,BRANCH_PK,PRTY_TYP,LOCATION_TYP) \r\n"
					+ "select PARTY_PK,OFFICER_PK,BANK_PK,null as BRANCH_PK,PRTY_TYP,'Bank' as LOCATION_TYP from X_PARTY_SURVIVORSHIP_TEMP";
			stmt.execute(sql);
			
			sql = "insert into C_PARTY_SURVIVORSHIP (PARTY_PK,OFFICER_PK,BANK_PK,BRANCH_PK,PRTY_TYP,LOCATION_TYP,BANKING_REL_ROWID,PARTY_REL_ROWID)\r\n"
					+ "select party_ldg.PARTY_PK,party_ldg.OFFICER_PK,party_ldg.BANK_PK,\r\n"
					+ "party_ldg.BRANCH_PK,party_ldg.PRTY_TYP,party_ldg.LOCATION_TYP,\r\n"
					+ "party_bnk.ROWID_OBJECT as BNK_REL_ROWID,party_rel.ROWID_OBJECT as party_rel_rowid\r\n"
					+ "from X_PRE_PARTY_SURVIVORSHIP party_ldg\r\n"
					+ "join C_XR_PRTY_BNKNG_REL party_bnk\r\n"
					+ "on party_ldg.PARTY_PK = party_bnk.X_RTY_FK and party_bnk.HUB_STATE_IND = '1' and \r\n"
					+ "party_ldg.LOCATION_TYP = 'Bank' and party_bnk.X_REL_TYPE_CODE like '%Bank%'\r\n"
					+ "join C_BR_PRTY_REL party_rel\r\n"
					+ "on party_ldg.PARTY_PK = party_rel.PRTY_FK_1 and party_ldg.LOCATION_TYP = 'Bank' and party_rel.HUB_STATE_IND = '1'";
			stmt.execute(sql);
			sql = "insert into C_PARTY_SURVIVORSHIP (PARTY_PK,OFFICER_PK,BANK_PK,BRANCH_PK,PRTY_TYP,LOCATION_TYP,BANKING_REL_ROWID,PARTY_REL_ROWID)\r\n"
					+ "select party_ldg.PARTY_PK,party_ldg.OFFICER_PK,party_ldg.BANK_PK,\r\n"
					+ "party_ldg.BRANCH_PK,party_ldg.PRTY_TYP,party_ldg.LOCATION_TYP,\r\n"
					+ "party_bnk.ROWID_OBJECT as BNK_REL_ROWID,party_rel.ROWID_OBJECT as party_rel_rowid\r\n"
					+ "from X_PRE_PARTY_SURVIVORSHIP party_ldg\r\n"
					+ "join C_XR_PRTY_BNKNG_REL party_bnk\r\n"
					+ "on party_ldg.PARTY_PK = party_bnk.X_RTY_FK and party_bnk.HUB_STATE_IND = '1' and \r\n"
					+ "party_ldg.LOCATION_TYP = 'Branch' and party_bnk.X_REL_TYPE_CODE like '%Branch%'\r\n"
					+ "join C_BR_PRTY_REL party_rel\r\n"
					+ "on party_ldg.PARTY_PK = party_rel.PRTY_FK_1 and party_ldg.LOCATION_TYP = 'Branch' and party_rel.HUB_STATE_IND = '1'";
			stmt.execute(sql);
			sql = "insert into C_PARTY_SURVIVORSHIP (PARTY_PK,OFFICER_PK,BANK_PK,BRANCH_PK,PRTY_TYP,LOCATION_TYP,BANKING_REL_ROWID,PARTY_REL_ROWID)\r\n"
					+ "select party_ldg.PARTY_PK,party_ldg.OFFICER_PK,party_ldg.BANK_PK,\r\n"
					+ "party_ldg.BRANCH_PK,party_ldg.PRTY_TYP,'Officer' as LOCATION_TYP,\r\n"
					+ "null as BNK_REL_ROWID,party_rel.ROWID_OBJECT as party_rel_rowid\r\n"
					+ "from X_PARTY_SURVIVORSHIP_TEMP party_ldg\r\n"
					+ "join C_BR_PRTY_REL party_rel\r\n"
					+ "on party_ldg.PARTY_PK = party_rel.PRTY_FK_1 and party_rel.HUB_STATE_IND = '1'";
			stmt.execute(sql);
			
			sql = "DROP TABLE IF EXISTS X_PARTY_SURVIVORSHIP_TEMP";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_PRE_PARTY_SURVIVORSHIP";
			stmt.execute(sql);
			
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}


	private static void proccessRule4() {
		System.out.println("\nProcessing Rule 4");
		try {
			Statement stmt = con.createStatement();
			String sql = "select PARTY_PKEY,OFFICER_PKEY,count(*) as OFFICER_COUNT\r\n"
					+ "into X_PARTY_SURV_RULE4\r\n"
					+ " from X_PARTY_SURV_MAIN group by PARTY_PKEY,OFFICER_PKEY";
			stmt.execute(sql);
			sql = " delete from X_PARTY_SURV_MAIN where concat(PARTY_PKEY,OFFICER_PKEY) not in(\r\n"
					+ " select concat(PARTY_PKEY,OFFICER_PKEY) from X_PARTY_SURV_RULE4 where concat(PARTY_PKEY,'|',OFFICER_COUNT) in\r\n"
					+ " (select concat(PARTY_PKEY,'|',MAX(OFFICER_COUNT)) from X_PARTY_SURV_RULE4 group by PARTY_PKEY))";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_PARTY_SURV_RULE4";
			stmt.execute(sql);
			
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}


	private static void proccessRule3() {
		System.out.println("\nProcessing Rule 3");
		try {
			Statement stmt = con.createStatement();
			String sql = "SELECT PARTY_PKEY,MAX(X_OPEN_DATE) as X_OPEN_DATE\r\n"
					+ "into X_PARTY_SURV_RULE3\r\n"
					+ "FROM X_PARTY_SURV_MAIN GROUP BY PARTY_PKEY ORDER BY PARTY_PKEY";
			stmt.execute(sql);
			sql = "delete from X_PARTY_SURV_MAIN where concat(PARTY_PKEY,X_OPEN_DATE) not in\r\n"
					+ "(select concat(PARTY_PKEY,X_OPEN_DATE) from X_PARTY_SURV_RULE3)";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_PARTY_SURV_RULE3";
			stmt.execute(sql);
			
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}


	private static void proccessRule2() {
		System.out.println("\nProcessing Rule 2");
		try {
			Statement stmt = con.createStatement();
			String sql = "select distinct PARTY_PKEY,X_SERV_CAT_CD,X_ACCOUNT_STATUS_GROUP,SERV_PRIORITY\r\n"
					+ "into X_PARTY_SURV_RULE2\r\n"
					+ "from X_PARTY_SURV_MAIN\r\n"
					+ "order by PARTY_PKEY";
			stmt.execute(sql);
			
			//Patch for fixing the issue with the role priority - 17/06/21
			sql = "delete from X_PARTY_SURV_MAIN where concat(PARTY_PKEY,'||',SERV_PRIORITY)  not in\r\n"
					+ "(select concat(PARTY_PKEY,'||',min(convert(int,SERV_PRIORITY))) as PKEY_PRIORITY from X_PARTY_SURV_RULE2 group by PARTY_PKEY)";
			
			/*
			 * sql =
			 * "delete from X_PARTY_SURV_MAIN where concat(PARTY_PKEY,'||',SERV_PRIORITY)  not in\r\n"
			 * +
			 * "(select concat(PARTY_PKEY,'||',min(SERV_PRIORITY)) as PKEY_PRIORITY from X_PARTY_SURV_RULE2 group by PARTY_PKEY)"
			 * ;
			 */
			
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_PARTY_SURV_RULE2";
			stmt.execute(sql);
			
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}		
	


	private static void populatePriority() {
		
		System.out.println("\nPopulating the priorities");
		String priority[][] =new String [34][2];
		int i;
		for(i=0;i<19;i++) {
			priority[i][0] = "OPEN";
		}
		for(i=19;i<34;i++) {
			priority[i][0] = "CLOSED";
		}
		priority[0][1] = "CN";
		priority[1][1] = "UL2";
		priority[2][1] = "OL";
		priority[3][1] = "DDA";
		priority[4][1] = "NOW";
		priority[5][1] = "OD";
		priority[6][1] = "UD3";
		priority[7][1] = "MMA";
		priority[8][1] = "SAV";
		priority[9][1] = "OS";
		priority[10][1] = "HEQ";
		priority[11][1] = "IL";
		priority[12][1] = "CL";
		priority[13][1] = "ODP";
		priority[14][1] = "CDA";
		priority[15][1] = "MC";
		priority[16][1] = "RA";
		priority[17][1] = "TR";
		priority[18][1] = "INV";
		priority[19][1] = "CN";
		priority[20][1] = "UL2";
		priority[21][1] = "OL";
		priority[22][1] = "DDA";
		priority[23][1] = "NOW";
		priority[24][1] = "OD";
		priority[25][1] = "MMA";
		priority[26][1] = "SAV";
		priority[27][1] = "CDA";
		priority[28][1] = "HEQ";
		priority[29][1] = "IL";
		priority[30][1] = "MC";
		priority[31][1] = "INV";
		priority[32][1] = "RA";
		priority[33][1] = "CL";
		
		//System.out.println("\nPrinting the priority");
		String sql = "update X_PARTY_SURV_MAIN set SERV_PRIORITY = ? where X_ACCOUNT_STATUS_GROUP = ? and X_SERV_CAT_CD = ?";
		
		for(i=0;i<34;i++) {
			System.out.println(i+1+". "+priority[i][1]+" - "+priority[i][0]);
			
			try {
				PreparedStatement stmt = con.prepareStatement(sql);
				stmt.setString(1, String.valueOf(i+1)); 
				stmt.setString(2, priority[i][0]);
				stmt.setString(3, priority[i][1]);
				stmt.execute();
			} 
			catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}


	private static void createMainTempTable() {
		System.out.println("\nCreating the main Temp Table");
		try {
			Statement stmt = con.createStatement();
			String sql = "select party.ROWID_OBJECT,party.ROWID_OBJECT as PARTY_PKEY,party.PRTY_TYP,\r\n"
					+ "acc.X_ACC_NMBR, acc.X_ACCOUNT_STATUS_GROUP,acc.X_SERV_CAT_CD,acc.X_OPEN_DATE,acc.x_loan_participation,\r\n"
					+ "product.X_PRODUCT_TYPE,\r\n"
					+ "bank.X_LOCATION_CD as banknbr, branch.X_LOCATION_CD as branchnbr,\r\n"
					+ "bank.ROWID_OBJECT as BANK_PKEY, branch.ROWID_OBJECT as BRANCH_PKEY, officer.ROWID_OBJECT as OFFICER_PKEY\r\n"
					+ "into X_PARTY_SURV_MAIN\r\n"
					+ "from\r\n"
					+ "C_BO_PRTY party  join\r\n"
					+ "C_XR_PRTY_ACC_REL acc_rel on party.ROWID_OBJECT=acc_rel.X_PRTY_FK and acc_rel.HUB_STATE_IND = '1' join\r\n"
					+ "C_XO_PRTY_ACCOUNT acc on acc.ROWID_OBJECT=acc_rel.x_Account_fk and acc.HUB_STATE_IND = '1'\r\n"
					+ "left join C_BO_PRTY officer on officer.ROWID_OBJECT = acc.X_BRANCH_REP_FK and officer.HUB_STATE_IND = '1'\r\n"
					+ "left join C_XO_PRODUCT product\r\n"
					+ "on product.ROWID_OBJECT = acc.X_PRODUCT_FK and product.HUB_STATE_IND = '1'\r\n"
					+ "left join C_XO_LOCATION bank on bank.ROWID_OBJECT = acc.X_BANK_FK and bank.HUB_STATE_IND = '1'\r\n"
					+ "left join C_XO_LOCATION branch on branch.ROWID_OBJECT = acc.X_BRANCH_FK and branch.HUB_STATE_IND = '1'\r\n"
					+ "where party.ROWID_OBJECT in \r\n"
					+ "(select  X_PRTY_FK as PARTY\r\n"
					+ "from C_XR_PRTY_ACC_REL where HUB_STATE_IND = '1' group by X_PRTY_FK having count(*) > 1)\r\n"
					+ "and party.HUB_STATE_IND = '1'\r\n"
					+ "order by party.ROWID_OBJECT,acc.X_BRANCH_REP_FK";
			stmt.execute(sql);
			sql = "ALTER TABLE X_PARTY_SURV_MAIN\r\n"
					+ "ADD SERV_PRIORITY varchar(5)";
			stmt.execute(sql);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}


	private static void proccessRule1() {
		System.out.println("\nProcessing Rule 1");
		try {
			Statement stmt = con.createStatement();
			String sql = "select distinct PARTY_PKEY,OFFICER_PKEY,BRANCH_PKEY,BANK_PKEY \r\n"
					+ "into X_PARTY_SURV_RULE1\r\n"
					+ "from X_PARTY_SURV_MAIN";
			stmt.execute(sql);
			sql = "insert into X_PARTY_SURVIVORSHIP_TEMP (PARTY_PK,OFFICER_PK,BANK_PK,BRANCH_PK,PRTY_TYP)\r\n"
					+ "select distinct PARTY_PKEY,OFFICER_PKEY,BANK_PKEY,BRANCH_PKEY,PRTY_TYP from X_PARTY_SURV_MAIN\r\n"
					+ "where PARTY_PKEY in (select PARTY_PKEY from X_PARTY_SURV_RULE1\r\n"
					+ "group by PARTY_PKEY\r\n"
					+ "having count(party_pkey)=1)";
			stmt.execute(sql);
			sql = "delete from X_PARTY_SURV_MAIN where PARTY_PKEY in (select PARTY_PKEY from X_PARTY_SURV_RULE1\r\n"
					+ "group by PARTY_PKEY\r\n"
					+ "having count(party_pkey)=1)";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_PARTY_SURV_RULE1";
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
			String sql = "DROP TABLE IF EXISTS X_PARTY_SURV_MAIN";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_PARTY_SURV_RULE1";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_PARTY_SURV_RULE2";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_PARTY_SURV_RULE3";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_PARTY_SURV_RULE4";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_PARTY_SURVIVORSHIP_TEMP";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_PRE_PARTY_SURVIVORSHIP";
			stmt.execute(sql);
			sql = "DROP TABLE IF EXISTS X_PARTY_SURV_PART_LOAN";
			stmt.execute(sql);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}

	private static void truncateLanding() {
		System.out.println("\nTruncating the Party Survivorship Landing Table");
		try {
			Statement stmt = con.createStatement();
			String sql = "truncate table C_PARTY_SURVIVORSHIP";
			stmt.execute(sql);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		
	}

	private static void processSingleAccountParties() {
		
		System.out.println("\nProcessing the single account party records");
		
		Statement stmt;
		try {
			
			stmt = con.createStatement();
			String sql = "select party.ROWID_OBJECT as PARTY_PK,officer.ROWID_OBJECT AS OFFICER_PK,\r\n"
					+ "bank.ROWID_OBJECT AS BANK_PK, branch.ROWID_OBJECT AS BRANCH_PK,party.PRTY_TYP\r\n"
					+ "into X_PARTY_SURVIVORSHIP_TEMP\r\n"
					+ "from\r\n"
					+ "C_BO_PRTY party  join\r\n"
					+ "C_XR_PRTY_ACC_REL acc_rel on party.ROWID_OBJECT=acc_rel.X_PRTY_FK and acc_rel.HUB_STATE_IND = '1' join\r\n"
					+ "C_XO_PRTY_ACCOUNT acc on acc.ROWID_OBJECT=acc_rel.x_Account_fk and acc.HUB_STATE_IND = '1'\r\n"
					+ "left join C_BO_PRTY officer on officer.ROWID_OBJECT = acc.X_BRANCH_REP_FK and officer.HUB_STATE_IND = '1'\r\n"
					+ "left join C_XO_PRODUCT product\r\n"
					+ "on product.ROWID_OBJECT = acc.X_PRODUCT_FK and product.HUB_STATE_IND = '1'\r\n"
					+ "left join C_XO_LOCATION bank on bank.ROWID_OBJECT = acc.X_BANK_FK and bank.HUB_STATE_IND = '1'\r\n"
					+ "left join C_XO_LOCATION branch on branch.ROWID_OBJECT = acc.X_BRANCH_FK and branch.HUB_STATE_IND = '1'\r\n"
					+ "where party.ROWID_OBJECT in \r\n"
					+ "(select  X_PRTY_FK as PARTY\r\n"
					+ "from C_XR_PRTY_ACC_REL where HUB_STATE_IND = '1' group by X_PRTY_FK having count(*) = 1)\r\n"
					+ "and party.HUB_STATE_IND = '1'\r\n"
					+ "order by party.ROWID_OBJECT,acc.X_BRANCH_REP_FK";
					
			stmt.execute(sql);
			
		} 
		catch (SQLException e) {
			
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
}
