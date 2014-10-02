import akka.actor._
import scala.math._
import scala.util.Random
import akka.actor.Actor._
import java.util.UUID
import java.lang.Long
import scala.collection.mutable._
import java.security.MessageDigest
import scala.collection.immutable.TreeMap

object project3bonus {
  val system =ActorSystem("pyproject2")
  case class join(ID:String,path:List[String],NetWork:HashMap[String, ActorRef])
  case class sendState(nodeID:String,path:List[String],lleaf:Array[String],rleaf:Array[String],routeT:Array[Array[String]])
  case class Msg(index:Int,from:String,nodeID:String,msgkey:String,path:String,hopnum:Int,NetWork:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  case class MsgArv(index:Int,nodeID:String,msgkey:String,path:String,hopnum:Int)
  case class finish(indexID:Int,sumhops:Int)
  case class Init(NetWork:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  case class initial(NetWork:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  case class sendMessage(network:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  case class Failure(cat:String,seq:Int,from:String,Init:String,key:String,path:String,hops:Int,network:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  case class Wakeup(from:String)
  	case class Contact(seq:Int,from:String,init:String,key:String,path:String,hops:Int,network:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  	case class fconn(NetWork:HashMap[String, ActorRef],mymap:TreeMap[Int,String],wakeup:Int)
  	case class fconnp(NetWork:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  	case class setMode(a:Int,b:List[(String,Int)])
  	case class failconf(id1:String,id2:String,fflag:Int)
  	
  /* generate random ip address excluding those already in the pastry network */
  def getIp(network: HashMap[String,ActorRef]): String = {
    val ip = scala.util.Random.nextInt(256) + "." + scala.util.Random.nextInt(256) + "." + scala.util.Random.nextInt(256) + "." + scala.util.Random.nextInt(256)
    if (!network.contains(ip)) ip else getIp(network)
  }
  
 /* /* generate nodeId, MD5 of its ip address */
  def md5(ip: String) = {
    val digest = MessageDigest.getInstance("MD5")
    digest.digest(ip.getBytes()).map("%02x".format(_)).mkString
  }*/
    def md5(num:Long): String ={
	 val leng = bits/b
	 val digit = Long.toString(num,math.pow(2,b).toInt)
	 var spl = ""
	 for(i <- 0 until leng-digit.length){
	   spl = spl+"0"
	 }
	 spl+digit
  }
  

 var testmode=false
 
 
 
  def main(args: Array[String]): Unit = {
    
    if(args.length < 2){
      println("Need two inputs!")
      return
    }
    if(args.length==3 && args(2)=="test"){
       testmode = true
    }
    val numNodes = args(0).toInt
    val numRequests = args(1).toInt
     var master=system.actorOf(Props(new Master(numNodes,numRequests,testmode)))  
  }
  
val bits=32
val b=4
val L=math.pow(2,b).toInt
class Node(nodeindex:Int, ID: String,numRequests:Int,master:ActorRef  ) extends Actor {

//class Node(nodeindex:Int, ID: String,numRequests:Int,NetWork:HashMap[String, ActorRef],mymap:TreeMap[Int,String],master:ActorRef  ) extends Actor {
   val indexID=nodeindex
   val nodeID=ID
  // val network=NetWork
  // val nodemap=mymap
   val boss=master
   val routeT=new Array[Array[String]](bits/b)
   val lleaf=new Array[String](L/2)
   val rleaf=new Array[String](L/2)
   var Arvcount=0
   var sumhops=0
   /////////////////
   var statemap= HashMap.empty[String,Int] //store the state of nodes
   val failconns = HashMap.empty[String,Int]
	  var diestate = 0 //0:not die node; 1:nodes with failed conns; 2: failed node
	 val retransmit = 3

   	  def setMode(state:Int,conns:List[(String,Int)]) = {
	    
	    diestate = state
	    if(conns!=null && conns.size>0){
	      conns.foreach( pair => failconns+= {pair._1 -> pair._2})
	    }
	  }
	  
   
   def findID(prefixR:String, Sortnode:Array[String],beginN:Int,endN:Int):String={
     for(m<-beginN until endN){
       if(Sortnode(m).startsWith(prefixR)) {return Sortnode(m)}       
     }
     return null
   }
   def initialize(network:HashMap[String, ActorRef],nodemap:TreeMap[Int,String])={
     val routecol=math.pow(2,b).toInt
     for(i<-0 until routeT.size){
       routeT(i)=new Array[String](routecol)
     }
     val Sortnode=nodemap.values.toArray
     util.Sorting.quickSort(Sortnode)
     val index=Sortnode.indexOf(nodeID) 
     // initialize the leaf table
    // println("Sortnode size="+ Sortnode.size+ " L is :"+L+ " index is :"+index)
     for(i<-0 until L/2){
       
        lleaf(i)={if(index>=L/2-i)Sortnode(index-L/2+i)else Sortnode(0)}
        rleaf(i)={if(index<Sortnode.size-i-1)Sortnode(index+i+1)else Sortnode.last}
        // println(nodeID+"lleaf"+i+" is :"+lleaf(i))
         //println(nodeID+"rleaf"+i+" is :"+rleaf(i))
     }
  
     // initialize the routetable
     for(i<-0 until routeT.size){
         var prefixR =nodeID.substring(0, i)
         var digitR=nodeID.substring(i, i+1)
         var IntdigitR=Integer.parseInt(digitR,16)
         for(j<-0 until routecol){
             if(j==IntdigitR){routeT(i)(j)=nodeID}
             else if(j<IntdigitR){routeT(i)(j)=findID(prefixR+Integer.toString(j,16),Sortnode,0,index)}
             else{
               if (index+1<Sortnode.size){routeT(i)(j)=findID(prefixR+Integer.toString(j,16),Sortnode,index+1,Sortnode.size)}
             }          
         }
         
     }
      //set status map for all elements in lset and rtable
	    lleaf.foreach(x => if(x!=nodeID) statemap+={x->0})
	    rleaf.foreach(x => if(x!=nodeID) statemap+={x->0})
	   
	    routeT.foreach( x => x.foreach(y => if(y!=nodeID&&y!=null) statemap+={y->0}) )
	    
     //println("route table is:"+routeT)
     self! sendMessage(network,nodemap)
   } //end initialize
   
def alter(k:String, orig:String, curr:String,nodemap:TreeMap[Int,String]):String = {
	    
	    //val lset = slset++llset
	    val sortN = nodemap.values.toArray
	    util.Sorting.quickSort(sortN)
	    val index = sortN.indexOf(orig)
	    
	    var result = orig
	    var resindex = index
	    if(index==0 || (k>orig && index<sortN.length-1)) {result=sortN(index+1);resindex=index+1}
		    else { result=sortN(index-1);resindex=index-1 }
		    
	    while(result==curr){
		  resindex += resindex-index
		  if(resindex==sortN.size){resindex=index-1}
		  else if(resindex==0){ resindex = index+1}
		  result = sortN(resindex)
	    }
	    
	    return result
	    
	  }
	  
   
   def getroute(thekey:String):String = { //find the node need to route to
     //println("key is:"+ thekey+"/n")
           if (thekey==null ||lleaf(0)==null||rleaf(0)==null||lleaf.last==null||rleaf.last==null){return nodeID}

       var cha=math.abs(Long.parseLong(thekey, 16)-Long.parseLong(nodeID, 16))
       //println("key is:"+ thekey+"cha is :"+cha+"/n")
       var Mindex= -1
       
       if(thekey>=lleaf(0) && thekey<=nodeID){ //if in the left leafs
            for(i<- 0 until lleaf.size){
               val tempcha=math.abs(Long.parseLong(thekey, 16)-Long.parseLong(lleaf(i), 16))
               if (tempcha<=cha){
                      cha=tempcha
                      Mindex=i
                      
                 }
            }
         if (Mindex<0) return nodeID else return lleaf(Mindex)
       }//end find in the left leaf
       else if(thekey > nodeID && thekey <= rleaf(rleaf.size-1)){ //find in the right leafs
           for (i<- 0 until rleaf.size){
                val tempcha=math.abs(Long.parseLong(thekey, 16)-Long.parseLong(rleaf(i), 16))
                if (tempcha<=cha){
                      cha=tempcha
                      Mindex=i                      
                 } 
           }
           if (Mindex<0) return nodeID else return rleaf(Mindex) 
       }
       else{  // find in the route table
         var mcp=(thekey,nodeID).zipped.takeWhile(Function.tupled(_==_)).map(_._1).mkString
         var lenOfmcp=mcp.length
         var digitR=Integer.parseInt(thekey.substring(lenOfmcp, lenOfmcp+1), 16) //first digit not in common
         if (routeT(lenOfmcp)(digitR)!=null){
             return routeT(lenOfmcp)(digitR)
         }
         else{
           val row=routeT(lenOfmcp) //search in the same row
           for(i<-0 until row.size){
               val tempcha=if (row(i)==null) cha else math.abs(Long.parseLong(thekey, 16)-Long.parseLong(row(i), 16))
               if (tempcha<cha){
                  cha=tempcha
                  Mindex=i
               }
           }
           if(Mindex>=0){return row(Mindex)}
           else if(thekey<nodeID) return lleaf(0)
           else return rleaf.last
         }         
       }     
   }
  def receive ={
    case Init(network,nodemap)=>initialize(network,nodemap) //then find A whose address is next to self
  
    case initial(network,nodemap)=>{            
                  if(indexID!=0){
                         var AID=nodemap(indexID-1)
                         network(AID)! join(nodeID,List(nodeID),network)                    
                                }
                     }
    case join(initalnode,path,network) => {
                 if (Long.parseLong(initalnode, 16)<Long.parseLong(nodeID, 16)){
                       util.Sorting.quickSort(lleaf)
                       if (Long.parseLong(initalnode, 16)>Long.parseLong(lleaf(0), 16)){lleaf(0)=initalnode}
                 }
                 else if (Long.parseLong(initalnode, 16)>Long.parseLong(nodeID, 16)){
                       util.Sorting.quickSort(rleaf)
                       if (Long.parseLong(initalnode, 16)<Long.parseLong(rleaf.last, 16)){rleaf(L/2-1)=initalnode}
                 }     

                 val routenode=getroute(initalnode)
                 if (routenode!=nodeID){
                   network(routenode)! join(initalnode,path++List(nodeID),network)
                   network(initalnode)! sendState(nodeID,path++List(nodeID),lleaf,rleaf,routeT)
                   //based on mymap update self table
                  
                 }
                 else {
                   network(initalnode)! sendState(nodeID,path++List(nodeID),lleaf,rleaf,routeT)
                   network(initalnode)! "lastjoin"
                   
                 } 
      
    }
    case sendState(pathnode,path,olleaf,orleaf,orouteT)=>{
      var lenOfpath=path.size
      routeT(lenOfpath-1)=orouteT(lenOfpath-1)  
      if (Long.parseLong(path.last, 16)<Long.parseLong(nodeID, 16)){
                       util.Sorting.quickSort(lleaf)
                       if (Long.parseLong(path.last, 16)>Long.parseLong(lleaf(0), 16)){lleaf(0)=path.last}
                 }
                 else if (Long.parseLong(path.last, 16)>Long.parseLong(nodeID, 16)){
                       util.Sorting.quickSort(rleaf)
                       if (Long.parseLong(path.last, 16)<Long.parseLong(rleaf.last, 16)){rleaf(L/2-1)=path.last}
                 } 
      
    }
   /* case "lastjoin"=> { // start to send message
          for(i<-1 to numRequests) {
               val routenode=getroute(nodeID)
               if (routenode==nodeID){
                 network(routenode)! MsgArv(i,nodeID,"null",0)
               }
               else {network(routenode)! Msg(i,nodeID,"",1)}
          }  
    }*/
    case fconn(fnetwork,fmymap,wakeup)=>{
   //    println("receive fconnp")
      val conleaf = lleaf++rleaf
      var id1=nodeID
      val id2 = conleaf((math.random*conleaf.size).toInt)
      val conns1 = List[(String,Int)]((id2,wakeup)); setMode(1, conns1)
      val conns2 = List[(String,Int)]((id1,wakeup)); fnetwork(id2)!setMode(1, conns2)
      master! failconf(id1,id2,1)
      println(" Connection dies temporarily: Node (" + id1 + ")<->N (" + id2 + ")")
    }
    case setMode(a,b)=>setMode(a,b)
    
    case fconnp(fnetwork,fmymap)=>{
     // println("receive fconnp")
      val conleaf = lleaf++rleaf
      var idp1=nodeID
      val idp2 = conleaf((math.random*conleaf.size).toInt)
      val connsp1 = List[(String,Int)]((idp2,-1)); setMode(1, connsp1)
      val connsp2 = List[(String,Int)]((idp1,-1)); fnetwork(idp2)!setMode(1, connsp2)
      master! failconf(idp1,idp2,2)
      println("  (" + idp1 + ") <-> (" + idp2 + ")  connection dies permanently")
    }
    case sendMessage(network,nodemap)=>{ 
               for(i<-1 to numRequests) {
               var msgkey=md5((math.random*mymax).toLong)
               val routenode=getroute(msgkey)
               if (routenode==nodeID){
                 network(routenode)! MsgArv(i,nodeID,msgkey,"null",0)
               }
               else {network(routenode)! Msg(i,nodeID,nodeID,msgkey,"",1,network,nodemap)}
          } 
      
    }
      
    case Msg(j,from,initnode,msgkey,path,hops,network,nodemap)=>{
  
               
	          if(diestate == 2 && (!statemap.contains(from) || statemap(from)>=0)){
	            network(from) ! Failure("Messg",j,nodeID,initnode,msgkey,path,hops,network,nodemap)
	          }
	          else if(diestate>0 && failconns.contains(from) && failconns(from)!=0){
	            network(from) ! Failure("Messg",j,nodeID,initnode,msgkey,path,hops,network,nodemap)
	            failconns +={from->(failconns(from)-1)}	            
	          }
	          else{	              
	              if(diestate>0 && failconns.contains(from) && failconns(from)==0){
		              failconns.remove(from)
		              if(diestate==1 && (failconns.size==0||failconns(from)==0)) { diestate=0 }
		              network(from) ! Wakeup(nodeID)
	              }
	            
	           val routenode=getroute(msgkey)
               if (routenode==nodeID){
                   network(initnode)! MsgArv(j,initnode,msgkey,path+nodeID,hops)
               }
               else {network(routenode)! Msg(j,nodeID,initnode,msgkey,path+nodeID+"+",hops+1,network,nodemap)} 

	          }

    }
    case MsgArv(j,initnode,msgkey,path,hops)=>{
           Arvcount+=1
           sumhops+=hops
           println("node "+nodeID+" receive the "+j+"th message, the path is "+path+" hops is "+hops)
           if(Arvcount==numRequests){//the node is finished
               master! finish(indexID,sumhops)
           }
                     
    }
    case "stop"=>exit()
    case Failure(cat,seq,from,initnode,key,path,hops,network,nodemap) => {
	          
	          //check whether the first time to meet the failure
	          if(statemap(from)<retransmit){
	              
	              println( "(" + nodeID + ") temporarily fails connectted with (" + from + ") " )
	            
		          statemap += {from->(statemap(from)+1)}

		          network(from) ! Msg(seq,nodeID,initnode,key,path+nodeID+"+",hops+1,network,nodemap)
	          }
	          else{
	            var next = alter(key,from,nodeID,nodemap)
	            
	            println( "(" + nodeID + ") permanently fails connectted with (" + from + ") !" +"\n"
	                + "use (" +next + ") replace " + from  )
	            
	            network(from) ! Contact(seq,nodeID,initnode,key,path,hops,network,nodemap)
	            
	          }
	}
	        
  case Wakeup(from) => {
	          println("N" + indexID + "(" + nodeID + ") find connect with N"  
	                + "(" + from + ") wake up!")
	        }
 case Contact(seq,from,initnode,msgkey,path,hops,network,nodemap) => {
	          if(diestate>0 && failconns.contains(from) && failconns(from)<0){
	            failconns.remove(from) 
	            if(diestate==1 && failconns.size==0) { diestate=0 }
	          }
	          else if(diestate==2){
	              
	            //println("add N"+map(from).intId + " to " + statemap(from))
	            statemap+=(from -> (-1))
	              
	          }
	          val routenode=getroute(msgkey)
               if (routenode==nodeID){
                   network(initnode)! MsgArv(seq,initnode,msgkey,path+nodeID,hops)
               }
               else {network(routenode)! Msg(seq,nodeID,initnode,msgkey,path+nodeID+"+",hops+1,network,nodemap)} 
	          	          	          
	        }
    case _=>{}
  } 

}
 val mymax=math.pow(2,bits).toLong
class Master(numNodes: Int, numRequests:Int,testmode:Boolean) extends Actor with ActorLogging {
    var count = 0
    var totalhops=0
     var NetWork = HashMap.empty[String, ActorRef] 
    var mymap = new TreeMap[Int,String]
   
  

    override def preStart() {
     // println(testmode)
        for (i <- 0 until numNodes){
                var IP=getIp(NetWork)
                var ID=md5((math.random*mymax).toLong)
                  mymap +={i->ID}
                
                var ss=i
                var node=system.actorOf(Props(new Node(ss,ID, numRequests, self)))
               NetWork +={ID->node}
                                
        }
        //println(mymap)
     for(allnodes<-NetWork.keysIterator){NetWork(allnodes)! Init(NetWork,mymap)}   
     if(testmode){      
      //generate randomly the connection who dies temperarily
    	 		 val wakeup = 2
    			 val n1 = (math.random*mymap.size+1).toInt
    			 NetWork(mymap(n1)) ! fconn(NetWork,mymap,wakeup)
    			 //val n1leaf = NetWork(mymap(n1)).slset++map(intmap(n1)).llset  收到消息再计算
                 var np1 = (math.random*mymap.size+1).toInt
                 while(np1==n1){ np1 = (math.random*mymap.size+1).toInt }
                 NetWork(mymap(np1)) ! fconnp(NetWork,mymap)
                 println("give die node")
         }   
    }
    var failcount=0
    var id1="";var id2="";var id3="";var id4=""
   def receive = {
       case failconf(n1,n2,flag)=>{
         if (flag==1){var id1=n1; var id2=n2;failcount =failcount+1}
         if (flag==2){var id3=n1; var id42=n2;failcount =failcount+1}
         if (failcount==2){
             var failnd = (math.random*mymap.size+1).toInt
             var failndID = mymap(failnd)
             while(List(id1,id2,id3,id4).contains(failndID)){ failnd = (math.random*mymap.size+1).toInt; failndID= mymap(failnd)}
             NetWork(failndID)!setMode(2,null)
             println("Node dies: N" + failnd + "(" + failndID + ")\n")
         }
         
       }
       case finish(nodeindex,nodehop) =>{
               totalhops+=nodehop
               count+=1
              // println(count)
              // println(nodeindex+" average hops is : "+(nodehop/numRequests.toDouble)+"\n")
               if (count==numNodes){
                 println("the whole network's average hops is :"+(totalhops/(numRequests.toDouble*numNodes.toDouble)))
                 for(allnode<-NetWork.keysIterator){NetWork(allnode)!"stop"}
                 exit()
               }
               
         
       }
       case _ =>{}
  }
 }
  
}
