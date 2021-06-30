package com.htlf.hraja.automation;

import java.io.File;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;



import com.htlf.hraja.survivorship.FlipCSI;
import com.htlf.hraja.survivorship.GroupContacts;
import com.htlf.hraja.survivorship.HouseholdSurvivorship;
import com.htlf.hraja.survivorship.PartySurvivorship;

import com.siperian.sif.client.SoapSiperianClient;
import com.siperian.sif.message.mrm.ExecuteBatchGroupRequest;
import com.siperian.sif.message.mrm.ExecuteBatchGroupResponse;

public class MDMAutomationWorkflow {
	
	private static SoapSiperianClient sipClient = null;
	private static Logger logger = Logger.getLogger("MDMAutomationWorkflow");  
	
	public static void main(String[] args) throws IOException {
		
		initializeLogger();
		logger.info("*************STARTING THE MDM AUTOMATED WORKFLOW*************");
		initializeSipClient();
		triggerFullLoad();
		triggerFullMnM();
		processFlipCSI();
		triggerSurvMnM();
		processPartySurv();
		processHouseholdSurv();
		triggerLoadSurv();
		processGroupContacts();
		triggerGroupContactLoad();
		logger.info("*************MDM AUTOMATED WORKFLOW FINISHED*************");
	}

	private static void initializeLogger() {
		try {
			
			File dir = new File("logs");
			System.out.println(dir.getAbsolutePath());
			
			//create the folder if not exists
			if(!dir.exists()) {
				//System.out.println("Logs folder does not exist");
				dir.mkdir();
			}
			
			FileHandler fh = new FileHandler("logs\\automation.log",true);
			
			dir = new File("logs\\automation.log");
			System.out.println("size of the log file = "+dir.length());
			
			//do not append if the log file exceeds 30 MB
			if(dir.length()>30000000) {
				fh = new FileHandler("logs\\automation.log",false);
			}
			
			logger.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();  
			fh.setFormatter(formatter);
			
			System.out.println("Logger initialized");
			
		} 
		catch (Exception e) {
			e.printStackTrace();
			
		}
		
	}

	private static void triggerGroupContactLoad() {
		
		System.out.println("\n\n\nTriggering the load group contacts batch group\n");
		logger.info("Triggering the load group contacts batch group");
		try {
			ExecuteBatchGroupRequest req = new ExecuteBatchGroupRequest();
			req.setBatchGroupUid("Load Group Contact");
			req.setResume(true);
			ExecuteBatchGroupResponse response = (ExecuteBatchGroupResponse) sipClient.process(req);
			System.out.println(response.getMessage());
			logger.info("load group contacts batch group: "+response.getMessage());
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.log(Level.SEVERE, null, e);
		}
		
	}

	private static void processGroupContacts() {
		System.out.println("\n\n\nCalling the group contacts class\n");
		logger.info("Calling the group contacts class");
		GroupContacts.main(null);
		logger.info("The group contacts class completed");
		
	}

	private static void triggerLoadSurv() {
		
		System.out.println("\n\n\nTriggering the Load Survivorship batch group\n");
		logger.info("Triggering the Load Survivorship batch group");
		try {
			ExecuteBatchGroupRequest req = new ExecuteBatchGroupRequest();
			req.setBatchGroupUid("Load Survivorship data");
			req.setResume(true);
			ExecuteBatchGroupResponse response = (ExecuteBatchGroupResponse) sipClient.process(req);
			System.out.println(response.getMessage());
			logger.info("Load Survivorship batch group: "+response.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			logger.log(Level.SEVERE, null, e);
		}
		
	}

	private static void processHouseholdSurv() {
		System.out.println("\n\n\nCalling the household survivorship class\n");
		logger.info("Calling the household survivorship class");
		HouseholdSurvivorship.main(null);
		logger.info("The party household survivorship completed");
	}

	private static void processPartySurv() {
		System.out.println("\n\n\nCalling the party survivorship class\n");
		logger.info("Calling the party survivorship class");
		PartySurvivorship.main(null);
		logger.info("The party survivorship class completed");
		
	}

	private static void triggerSurvMnM() {
		
		System.out.println("\n\n\nTriggering the Survivorship M&M batch group\n");
		logger.info("Triggering the Survivorship M&M batch group");
		
		try {
			ExecuteBatchGroupRequest req = new ExecuteBatchGroupRequest();
			req.setBatchGroupUid("Match and Merge Survivorship");
			req.setResume(true);
			ExecuteBatchGroupResponse response = (ExecuteBatchGroupResponse) sipClient.process(req);
			System.out.println(response.getMessage());
			logger.info("Survivorship M&M batch group: "+response.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			logger.log(Level.SEVERE, null, e);
		}
		
	}

	private static void processFlipCSI() {
		System.out.println("\n\n\nCalling the flip CSI class\n");
		logger.info("Calling the flip CSI class");
		FlipCSI.main(null);
		logger.info("The flip CSI class Completed");
	}

	private static void triggerFullMnM() {
		
		System.out.println("\n\n\nTriggering the full M&M batch group\n");
		logger.info("Triggering the full M&M batch group");
		
		try {
			ExecuteBatchGroupRequest req = new ExecuteBatchGroupRequest();
			req.setBatchGroupUid("Full Match and Merge");
			req.setResume(true);
			ExecuteBatchGroupResponse response = (ExecuteBatchGroupResponse) sipClient.process(req);
			System.out.println(response.getMessage());
			logger.info("full M&M batch group: "+response.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			logger.log(Level.SEVERE, null, e);
		}
		
	}

	private static void triggerFullLoad() {
		
		System.out.println("\n\n\nTriggering the full load batch group\n");
		logger.info("Triggering the full load batch group");
		
		try {
			ExecuteBatchGroupRequest req = new ExecuteBatchGroupRequest();
			req.setBatchGroupUid("Load All Data");
			req.setResume(true);
			ExecuteBatchGroupResponse response = (ExecuteBatchGroupResponse) sipClient.process(req);
			System.out.println(response.getMessage());
			logger.info("full load batch group: "+response.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			logger.log(Level.SEVERE, null, e);
		}
		
	}
	
	private static void initializeSipClient() throws IOException {
		
		System.out.println("Initializing the Soap Siperian Client");
		logger.info("Initializing the Soap Siperian Client");
		
		FileReader fr = new FileReader("automation.properties");
		
		
		Properties properties = new Properties();
		properties.load(fr);
		/*
		 * properties.put(SiperianClient.SIPERIANCLIENT_PROTOCOL, "soap");
		 * properties.put(SoapSiperianClient.SOAP_CALL_TIMEOUT, "60000000");
		 * properties.put("siperian-client.orsId", "10.4.90.81-CUST_ORS");
		 * properties.put("siperian-client.username", "admin");
		 * properties.put("siperian-client.password", "admin");
		 * properties.put("soap.call.url",
		 * "http://10.4.90.246:8080/cmx/services/SifService");
		 */
        
        //sipClient = (SoapSiperianClient) SiperianClient.newSiperianClient(properties);
		
		sipClient = new SoapSiperianClient(properties);
		
	}


}
