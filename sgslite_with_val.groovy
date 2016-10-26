config create_schema: false
config load_new: false	


dbinput = File.csv(filename).delimiter(",").header('TimeStamp', 'SRCIP', 'DESTIP', 'IMSINum', 'TAI', 'ECGI', 'Msg_Dir', 'BatchTime', 'Interface').expand {
it["Msg_Dir"] = it["Msg_Dir"].toInteger() 
return it["Msg_Dir"] != 0 ? [it] : [] 
}




load(dbinput).asVertices{
        label "IMSI"
        ignore "TimeStamp"
        ignore "SRCIP"
        ignore "DESTIP"
        ignore "TAI"
        ignore "ECGI"        
        ignore "Msg_Dir"
        ignore "BatchTime" 
        ignore "Interface"
        key "IMSINum"

}


load(dbinput).asVertices{
        label "CELLID"
        ignore "TimeStamp"
        ignore "SRCIP"
        ignore "DESTIP"
        ignore "IMSINum"
        ignore "TAI"
        ignore "Msg_Dir"
        ignore "BatchTime"
        ignore "Interface"
        key "ECGI"
}


load(dbinput).asVertices{
        label "TR"
        ignore "TimeStamp"
        ignore "SRCIP"
        ignore "DESTIP"        
        ignore "IMSINum"
        ignore "ECGI"        
        ignore "Msg_Dir"
        ignore "BatchTime"
        ignore "Interface"
        key "TAI"
}


load(dbinput).asVertices{
        label "VMME"
        ignore "TimeStamp"
        ignore "IMSINum"
        ignore "DESTIP"        
        ignore "TAI"
        ignore "ECGI"        
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
        ignore "TAI"
        ignore "ECGI"        
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
        ignore "TAI"
        ignore "ECGI"        
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
    label "ConnectedTo"
	ignore "TimeStamp"	
        ignore "SRCIP"
        ignore "DESTIP"
        ignore "IMSINum"        
        ignore "TAI"
        ignore "ECGI"        
        ignore "Msg_Dir"
        ignore "BatchTime"  
        ignore "Interface"

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
	ignore "TimeStamp"	
        ignore "SRCIP"
        ignore "DESTIP"
        ignore "IMSINum"       
        ignore "TAI"
        ignore "ECGI"       
        ignore "Msg_Dir"
        ignore "BatchTime"  
        ignore "Interface"

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
    label "LocatedIn"
       ignore "TimeStamp"	
        ignore "SRCIP"
        ignore "DESTIP"
        ignore "IMSINum"       
        ignore "TAI"
        ignore "ECGI"       
        ignore "Msg_Dir"
        ignore "BatchTime"  
        ignore "Interface"
    outV "IMSINum", {
        label "IMSI"
                key "IMSINum"
    }
    inV "ECGI", {
        label "CELLID"
               key "ECGI"
    }
}



load(dbinput).asEdges {
    label "Using"
        ignore "TimeStamp"	
        ignore "SRCIP"
        ignore "DESTIP"
        ignore "IMSINum"       
        ignore "TAI"
        ignore "ECGI"       
        ignore "Msg_Dir"
        ignore "BatchTime"  
        ignore "Interface"
    outV "IMSINum", {
        label "IMSI"
         key "IMSINum"
    }
    inV "SRCIP", {
		label "VMME"
		key "SRCIP"
	       
	    }
}








































