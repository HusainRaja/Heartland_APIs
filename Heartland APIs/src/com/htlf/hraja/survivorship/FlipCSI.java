package com.htlf.hraja.survivorship;

import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class FlipCSI {

	private static Connection con = null;
	
	public static void main(String[] args) {
		
		System.out.println("Running the FlipCSI Code\n");
		
		//Initialize the DB Connection
		initializeConnection();
		
		//Process records and flip consolidation ind
		procesAndFlipCSI();
		
		//Closing the Connection
		closeConnection();

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


	private static void procesAndFlipCSI() {
		
		try {
			
			System.out.println("\nProcessing the Party Banking records");
			Statement stmt = con.createStatement();
			String sql = "  update C_XR_PRTY_BNKNG_REL set CONSOLIDATION_IND = '4' where X_RTY_FK in\r\n"
					+ " (select X_RTY_FK from C_XR_PRTY_BNKNG_REL where X_REL_TYPE_CODE like '%Bank%' group by X_RTY_FK having count(*) >1)";
			stmt.execute(sql);
			sql = "  update C_XR_PRTY_BNKNG_REL set CONSOLIDATION_IND = '4' where X_RTY_FK in\r\n"
					+ " (select X_RTY_FK from C_XR_PRTY_BNKNG_REL where X_REL_TYPE_CODE like '%Branch%' group by X_RTY_FK having count(*) >1)";
			stmt.execute(sql);
			
			System.out.println("\nProcessing the Party Rel records");
			sql = " update C_BR_PRTY_REL set CONSOLIDATION_IND = '4' where PRTY_FK_1 in\r\n"
					+ " (select PRTY_FK_1 from C_BR_PRTY_REL group by PRTY_FK_1 having count(*) >1)";
			stmt.execute(sql);
			
			System.out.println("\nProcessing the Group Banking records");
			sql = "update C_XR_PRTY_GRP_BNKNG_REL set CONSOLIDATION_IND = '4' where X_GROUP_FK in\r\n"
					+ "(select X_GROUP_FK from C_XR_PRTY_GRP_BNKNG_REL where X_REL_TYPE_CODE like '%Branch%' group by X_GROUP_FK having count(*) >1)";
			stmt.execute(sql);
			sql = "update C_XR_PRTY_GRP_BNKNG_REL set CONSOLIDATION_IND = '4' where X_GROUP_FK in\r\n"
					+ "(select X_GROUP_FK from C_XR_PRTY_GRP_BNKNG_REL where X_REL_TYPE_CODE like '%Bank%' group by X_GROUP_FK having count(*) >1)";
			stmt.execute(sql);
			
			System.out.println("\nProcessing the Group Rel records");
			sql = "update C_BR_PRTY_GRP_REL set CONSOLIDATION_IND = '4' where PRTY_GRP_FK in\r\n"
					+ "(select PRTY_GRP_FK from C_BR_PRTY_GRP_REL where REL_TYPE_CODE like '%Officer%' group by PRTY_GRP_FK having count(*) >1)";
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

}
