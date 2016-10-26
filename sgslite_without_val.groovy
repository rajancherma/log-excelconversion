config create_schema: false
config load_new: false	


dbinput = File.csv(filename).delimiter(",").header('TimeStamp', 'SRCIP', 'DESTIP', 'IMSINum',  'Msg_Dir', 'BatchTime', 'Interface').expand {
it["Msg_Dir"] = it["Msg_Dir"].toInteger() 
return it["Msg_Dir"] != 0 ? [it] : [] 
}



load(dbinput).asVertices{
        label "IMSI"
        ignore "TimeStamp"
        ignore "SRCIP"
        ignore "DESTIP"
        ignore "Msg_Dir"
        ignore "BatchTime" 
        ignore "Interface"
        key "IMSINum"

}


load(dbinput).asVertices{
        label "VMME"
        ignore "TimeStamp"
        ignore "IMSINum"
        ignore "DESTIP"        
        ignore "Msg_Dir"
        ignore "BatchTime"  
        ignore "Interface"
        key "SRCIP"
}




load(dbinput).asVertices{
        label "GWTS"
        ignore "TimeStamp"	
        ignore "SRCIP"
        ignore "IMSINum"        
        ignore "Msg_Dir"
        ignore "BatchTime"
        ignore "Interface"
        key "DESTIP"
}


load(dbinput).asEdges {
    label "ConnectedTo"
	ignore "TimeStamp"	
        ignore "SRCIP"
        ignore "DESTIP"
        ignore "IMSINum"       
        ignore "Msg_Dir"
        ignore "BatchTime"       
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
    label "Using"
        ignore "SRCIP"
        ignore "DESTIP"
        ignore "IMSINum"        
        ignore "Msg_Dir"
         ignore "Interface"
         ignore "TimeStamp"	
          ignore "BatchTime"       
    outV "IMSINum", {
        label "IMSI"
                key "IMSINum"
    }
    inV "SRCIP", {
		label "VMME"
		key "SRCIP"
	       
	    }
}







































