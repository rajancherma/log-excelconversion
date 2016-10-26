config create_schema: true	
config load_new: false	


dbinput = File.csv(filename).delimiter(",").header('TimeStamp', 'SRCIP', 'DESTIP', 'Command_Code', 'Application_Id','Command_Flag', 'Msg_Type', 'Msg_Dir', 'Hop_Identifier', 'EtoE_identifier','CC_Req_Type','Session_Id','Origin_Host', 'Destination_Host', 'UserName', 'Visitied_PLMN_Id', 'Result_Code','Cancel_Type', 'Last_UE_Activation_time', 'Trace_ref', 'Visited_Network_Id', 'BatchTime' ,'pcap' ,'Interface').expand {
it["Msg_Dir"] = it["Msg_Dir"].toInteger() 
return it["Msg_Dir"] != 0 ? [it] : [] 
}


load(dbinput).asVertices{
        label "IMSI"
        ignore "TimeStamp"
        ignore "SRCIP"
        ignore "DESTIP"  
        ignore "Command_Code"      
        ignore "Application_Id"
        ignore "Command_Flag"
        ignore "Msg_Type"
        ignore "Msg_Dir"
        ignore "Hop_Identifier"
        ignore "EtoE_identifier"
        ignore "CC_Req_Type"
        ignore "Session_Id"
        ignore "Origin_Host"
        ignore "Destination_Host"
        ignore "UserName"
        ignore "Visitied_PLMN_Id"
        ignore "Result_Code"
        ignore "Cancel_Type"
        ignore "Last_UE_Activation_time"
        ignore "Visited_Network_Id"
        ignore "BatchTime"
        ignore "pcap"
        ignore "Interface"
        key "IMSINum"

}




load(dbinput).asVertices{
        label "VMME"
         ignore "TimeStamp"
        ignore "IMSINum"
        ignore "DESTIP"  
        ignore "Command_Code"      
        ignore "Application_Id"
        ignore "Command_Flag"
        ignore "Msg_Type"
        ignore "Msg_Dir"
        ignore "Hop_Identifier"
        ignore "EtoE_identifier"
        ignore "CC_Req_Type"
        ignore "Session_Id"
        ignore "Origin_Host"
        ignore "Destination_Host"
        ignore "UserName"
        ignore "Visitied_PLMN_Id"
        ignore "Result_Code"
        ignore "Cancel_Type"
        ignore "Last_UE_Activation_time"
        ignore "Visited_Network_Id"
        ignore "BatchTime"
        ignore "pcap"
        ignore "Interface"
        key "SRCIP"
}



load(dbinput).asVertices{
        label "HSS"
         ignore "TimeStamp"
        ignore "IMSINum"
        ignore "SRCIP"  
        ignore "Command_Code"      
        ignore "Application_Id"
        ignore "Command_Flag"
        ignore "Msg_Type"
        ignore "Msg_Dir"
        ignore "Hop_Identifier"
        ignore "EtoE_identifier"
        ignore "CC_Req_Type"
        ignore "Session_Id"
        ignore "Origin_Host"
        ignore "Destination_Host"
        ignore "UserName"
        ignore "Visitied_PLMN_Id"
        ignore "Result_Code"
        ignore "Cancel_Type"
        ignore "Last_UE_Activation_time"
        ignore "Visited_Network_Id"
        ignore "BatchTime"
        ignore "pcap"
         ignore "Interface"
        key "DESTIP"
}

load(dbinput).asEdges {
    label "ConnectedTo"
           ignore "TimeStamp"
        ignore "IMSINum"
        ignore "DESTIP"
        ignore "SRCIP"  
        ignore "Command_Code"      
        ignore "Application_Id"
        ignore "Command_Flag"
        ignore "Msg_Type"
        ignore "Msg_Dir"
        ignore "Hop_Identifier"
        ignore "EtoE_identifier"
        ignore "CC_Req_Type"
        ignore "Session_Id"
        ignore "Origin_Host"
        ignore "Destination_Host"
        ignore "UserName"
        ignore "Visitied_PLMN_Id"
        ignore "Result_Code"
        ignore "Cancel_Type"
        ignore "Last_UE_Activation_time"
        ignore "Visited_Network_Id"
        ignore "BatchTime"
        ignore "pcap"
    outV "SRCIP", {
        label "VMME"
        key "SRCIP"
      
    }
    inV "DESTIP", {
        label "HSS"
        key "DESTIP"
	
    }
}

load(dbinput).asEdges {
    	label "Using"
 	ignore "TimeStamp"
        ignore "IMSINum"
        ignore "DESTIP"
        ignore "SRCIP"  
        ignore "Command_Code"      
        ignore "Application_Id"
        ignore "Command_Flag"
        ignore "Msg_Type"
        ignore "Msg_Dir"
        ignore "Hop_Identifier"
        ignore "EtoE_identifier"
        ignore "CC_Req_Type"
        ignore "Session_Id"
        ignore "Origin_Host"
        ignore "Destination_Host"
        ignore "UserName"
        ignore "Visitied_PLMN_Id"
        ignore "Result_Code"
        ignore "Cancel_Type"
        ignore "Last_UE_Activation_time"
        ignore "Visited_Network_Id"
        ignore "BatchTime"
         ignore "Interface"
        ignore "pcap"
	    outV "IMSINum", {
		label "IMSI"
		key "IMSINum"
	
	    }
	    inV "SRCIP", {
		label "VMME"
		key "SRCIP"
	       
	    }
}










































