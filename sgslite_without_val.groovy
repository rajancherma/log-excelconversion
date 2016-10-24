config create_schema: true	
config load_new: false	

//def inputfiledir = '/home/verizon/FINALCSV/SGSLITE/testdup2.csv'

dbinput = File.csv(filename).delimiter(",").header('TimeStamp', 'SRCIP', 'DESTIP', 'Msg_Type', 'Msg_ID', 'IMSINum', 'MME', 'TAI', 'ECGI', 'PI', 'DVC_G_Id', 'recovery', 'Cause', 'NAS', 'Msg_Dir', 'BatchTime', 'pcap').expand {
it["Msg_Dir"] = it["Msg_Dir"].toInteger() 
return it["Msg_Dir"] != 0 ? [it] : [] 
}


load(dbinput).asVertices{
        label "IMSI"
        ignore "TimeStamp"
        ignore "SRCIP"
        ignore "DESTIP"
        ignore "Msg_Type"
        ignore "Msg_ID"
        ignore "MME"
        ignore "TAI"
        ignore "ECGI"
        ignore "PI"
        ignore "DVC_G_Id"
        ignore "recovery"
        ignore "Cause"
        ignore "Msg_Dir"
        ignore "BatchTime"
        ignore "pcap"
        ignore "NAS"
        key "IMSINum"

}




load(dbinput).asVertices{
        label "VMME"
        ignore "TimeStamp"
        ignore "IMSI"
        ignore "DESTIP"
        ignore "Msg_Type"
        ignore "Msg_ID"
        ignore "MME"
        ignore "TAI"
        ignore "ECGI"
        ignore "PI"
        ignore "DVC_G_Id"
        ignore "recovery"
        ignore "Cause"
        ignore "Msg_Dir"
        ignore "BatchTime"
        ignore "pcap"
	ignore "NAS"
        key "SRCIP"
}



load(dbinput).asVertices{
        label "GWTS"
        ignore "TimeStamp"	
        ignore "SRCIP"
        ignore "IMSI"
        ignore "Msg_Type"
        ignore "Msg_ID"
        ignore "MME"
        ignore "TAI"
        ignore "ECGI"
        ignore "PI"
        ignore "DVC_G_Id"
        ignore "recovery"
        ignore "Cause"
        ignore "Msg_Dir"
        ignore "BatchTime"
        ignore "pcap"
	ignore "NAS"
        key "DESTIP"
}

load(dbinput).asEdges {
    label "Relates"
        ignore "TimeStamp"	
        ignore "SRCIP"
        ignore "DESTIP"
        ignore "IMSI"
        ignore "Msg_Type"
        ignore "Msg_ID"
        ignore "MME"
        ignore "TAI"
        ignore "ECGI"
        ignore "PI"
        ignore "DVC_G_Id"
        ignore "recovery"
        ignore "Cause"
        ignore "Msg_Dir"
        ignore "BatchTime"
        ignore "pcap"
	ignore "NAS"
    outV "SRCIP", {
        label "VMME"
        key "SRCIP"
      
    }
    inV "DESTIP", {
        label "GWTS"
        key "DESTIP"
	
    }
}

load(dbinput).asEdges {
    	label "Having"
 	ignore "IMSI"
        ignore "DESTIP"
        ignore "Msg_Type"
        ignore "Msg_ID"
        ignore "MME"
        ignore "TAI"
        ignore "ECGI"
        ignore "PI"
        ignore "DVC_G_Id"
        ignore "recovery"
        ignore "Cause"
        ignore "Msg_Dir"
        ignore "pcap"
	ignore "NAS"
        ignore "SRCIP"
	    outV "IMSI", {
		label "IMSI"
		key "IMSI"
	
	    }
	    inV "SRCIP", {
		label "VMME"
		key "SRCIP"
	       
	    }
}










































