config create_schema: true
config load_new: false	

//def inputfiledir = '/home/verizon/FINALCSV/SGSLITE/'

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
        label "CELLID"
        ignore "TimeStamp"
        ignore "SRCIP"
        ignore "DESTIP"
        ignore "Msg_Type"
        ignore "IMSINum"
        ignore "MME"
        ignore "TAI"
        ignore "PI"
        ignore "DVC_G_Id"
        ignore "recovery"
        ignore "Cause"
        ignore "Msg_Dir"
        ignore "BatchTime"
        ignore "pcap"
ignore "NAS"
key "ECGI"
}


load(dbinput).asVertices{
        label "TR"
        ignore "TimeStamp"
        ignore "SRCIP"
        ignore "DESTIP"
        ignore "Msg_Type"
        ignore "Msg_ID"
        ignore "MME"
        ignore "IMSINum"
        ignore "ECGI"
        ignore "PI"
        ignore "DVC_G_Id"
        ignore "recovery"
        ignore "Cause"
        ignore "Msg_Dir"
        ignore "BatchTime"
        ignore "pcap"
ignore "NAS"
        key "TAI"
}


load(dbinput).asVertices{
        label "VMME"
        ignore "TimeStamp"
        ignore "IMSINum"
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
        ignore "IMSINum"
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
        ignore "IMSINum"
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
    label "Includes"
ignore "Ts"	
        ignore "SRCIP"
        ignore "DESTIP"
        ignore "IMSINum"
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
    outV "TAI", {
        label "TR"
                key "TAI"
    }
    inV "SRCIP", {
        label "VMME"
               key "SRCIP"
    }
}


load(dbinput).asEdges {
    label "presentIn"
ignore "Ts"	
        ignore "SRCIP"
        ignore "DESTIP"
        ignore "IMSINum"
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
    outV "ECGI", {
        label "CELLID"
                key "ECGI"
    }
    inV "TAI", {
        label "TR"
               key "TAI"
    }
}


load(dbinput).asEdges {
    label "Location"
ignore "Ts"	
        ignore "SRCIP"
        ignore "DESTIP"
        ignore "IMSINum"
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
    outV "IMSI", {
        label "IMSI"
                key "IMSINum"
    }
    inV "ECGI", {
        label "CELLID"
               key "ECGI"
    }
}







































