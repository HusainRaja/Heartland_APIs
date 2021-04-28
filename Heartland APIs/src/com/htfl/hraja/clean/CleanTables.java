package com.htfl.hraja.clean;

import java.util.Properties;
import java.util.Scanner;

import com.siperian.sif.client.SiperianClient;
import com.siperian.sif.client.SoapSiperianClient;
import com.siperian.sif.message.mrm.CleanTableRequest;
import com.siperian.sif.message.mrm.CleanTableResponse;


public class CleanTables {
	
	static SoapSiperianClient sipClient = null;

	public static void main(String[] args) {
		
		Scanner sc = new Scanner(System.in); 
		//Tables which are to be cleaned.
		String tables[] = {"C_BO_PRTY_RLE_COMM_PREF","C_BO_PRTY_RLE_PHN_COMM","C_BO_PRTY_RLE_ETRNC_ADDR","C_BR_PRTY_RLE_PSTL_ADDR","C_XR_PRODUCT_REL","C_XR_LOCATION_REL",
				"C_XR_PRTY_GRP_BNKNG_REL","C_XR_PRTY_BNKNG_REL","C_XO_PRTY_PROFILE","C_XR_PRTY_ACC_REL","C_XO_PRTY_ACCOUNT","C_BR_PRTY_REL","C_BR_PRTY_GRP_REL",
				"C_BO_PRTY_RLE_TAX","C_BO_PRTY_RLE_ALT_ID","C_BO_PSTL_ADDR","C_XO_LOCATION","C_XO_PRODUCT","C_BO_PRTY_GRP","C_BO_PRTY"};
		
		System.out.println("Clean all Tables? (Y/N): ");
		String ch = sc.nextLine();
		
		//Initializing the Siperian SOAP client
		initializeSipClient();
		
		//Initializing the request for Clean Table SIF API
		CleanTableRequest request = new CleanTableRequest();
		request.setUseTruncate(true);
		
		if(ch.equalsIgnoreCase("Y")) {
			for(String tableName:tables) {
				try {
					System.out.println("\nCleaning the table: "+tableName);
					request.setSiperianObjectUid("BASE_OBJECT."+tableName);
					CleanTableResponse response = (CleanTableResponse) sipClient.process(request);
					System.out.println(response.getMessage());
				}
				catch (Exception e) {
					
					e.printStackTrace();
				}
				
			}
		}
		else {
			System.out.println("Terminating the program");
		}
		
	}
	
	private static void initializeSipClient() {
		Properties properties = new Properties();
		properties.put(SiperianClient.SIPERIANCLIENT_PROTOCOL, "soap");
		properties.put("siperian-client.orsId", "10.4.90.81-CUST_ORS");
		properties.put("siperian-client.username", "admin");
		properties.put("siperian-client.password", "admin");
		properties.put("soap.call.url", "http://10.4.90.246:8080/cmx/services/SifService");
        
        sipClient = (SoapSiperianClient) SiperianClient.newSiperianClient(properties);
		
	}

}
