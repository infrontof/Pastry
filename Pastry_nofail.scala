import akka.actor._
import scala.math._
import scala.util.Random
import akka.actor.Actor._
import java.util.UUID
import java.lang.Long
import scala.collection.mutable._
import java.security.MessageDigest
import scala.collection.immutable.TreeMap

object project3 {
  val system =ActorSystem("pyproject2")
  case class join(ID:String,path:List[String],NetWork:HashMap[String, ActorRef])
  case class sendState(nodeID:String,path:List[String],lleaf:Array[String],rleaf:Array[String],routeT:Array[Array[String]])
  case class Msg(index:Int,nodeID:String,msgkey:String,path:String,hopnum:Int,NetWork:HashMap[String, ActorRef])
  case class MsgArv(index:Int,nodeID:String,msgkey:String,path:String,hopnum:Int)
  case class finish(indexID:Int,sumhops:Int)
  case class Init(NetWork:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  case class initial(NetWork:HashMap[String, ActorRef],mymap:TreeMap[Int,String])
  case class sendMessage(network:HashMap[String, ActorRef])
  case class lastjoin(lleaf:Array[String],rleaf:Array[String],NetWork:HashMap[String, ActorRef])
  
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
  

 
  def main(args: Array[String]): Unit = {
    
    if(args.length < 2){
      println("Need two inputs!")
      return
    }
    
    val numNodes = args(0).toInt
    val numRequests = args(1).toInt
     var master=system.actorOf(Props(new Master(numNodes,numRequests)))  
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
       //  println(nodeID+"rleaf"+i+" is :"+rleaf(i))
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
     //println("route table is:"+routeT)
     self! sendMessage(network)
   } //end initialize
   def getroute(thekey:String):String = { //find the node need to route to
     //println("key is:"+ thekey+"/n")
      if (thekey==null ||lleaf(0)==null||rleaf(0)==null||lleaf.last==null||rleaf.last==null){return nodeID}
       var cha=math.abs(Long.parseLong(thekey, 16)-Long.parseLong(nodeID, 16))
      // println("key is:"+ thekey+"cha is :"+cha+"/n")
       var Mindex= -1
       if(Long.parseLong(thekey, 16)>=Long.parseLong(lleaf(0), 16) && Long.parseLong(thekey, 16)<=Long.parseLong(nodeID, 16)){ //if in the left leafs
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
              /*    if(indexID!=0){
                    var AID=nodemap(indexID-1)
                    network(AID)! join(nodeID,List(nodeID),network)
                    
                  }*/
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
                   network(initalnode)! lastjoin(lleaf,rleaf,network)
                   
                 } //final node of join path的
      
    }
    case sendState(pathnode,path,olleaf,orleaf,orouteT)=>{//update
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
    case lastjoin(olleaf,orleaf,network)=> { // updateleaf
       
          for(i<-0 until lleaf.size){
                  if (Long.parseLong(olleaf(i), 16)<Long.parseLong(nodeID, 16)){
                       util.Sorting.quickSort(lleaf)
                       if (Long.parseLong(olleaf(i), 16)>Long.parseLong(lleaf(0), 16)){lleaf(0)=olleaf(i)}
                 }
                 else if (Long.parseLong(olleaf(i), 16)>Long.parseLong(nodeID, 16)){
                       util.Sorting.quickSort(rleaf)
                       if (Long.parseLong(olleaf(i), 16)<Long.parseLong(rleaf.last, 16)){rleaf(L/2-1)=olleaf(i)}
                 } 
                 if (Long.parseLong(orleaf(i), 16)<Long.parseLong(nodeID, 16)){
                       util.Sorting.quickSort(lleaf)
                       if (Long.parseLong(orleaf(i), 16)>Long.parseLong(lleaf(0), 16)){lleaf(0)=orleaf(i)}
                 }
                 else if (Long.parseLong(orleaf(i), 16)>Long.parseLong(nodeID, 16)){
                       util.Sorting.quickSort(rleaf)
                       if (Long.parseLong(orleaf(i), 16)<Long.parseLong(rleaf.last, 16)){rleaf(L/2-1)=orleaf(i)}
                 } 
          }
          self!sendMessage(network)
    }
    case sendMessage(network)=>{ 
               for(i<-1 to numRequests) {
               var msgkey=md5((math.random*mymax).toLong)
               val routenode=getroute(msgkey)
               if (routenode==nodeID){
                 network(routenode)! MsgArv(i,nodeID,msgkey,"null",0)
               }
               else {network(routenode)! Msg(i,nodeID,msgkey,"",1,network)}
          } 
      
    }
      
    case Msg(j,initnode,msgkey,path,hops,network)=>{
               val routenode=getroute(msgkey)
               if (routenode==nodeID){
                   network(initnode)! MsgArv(j,initnode,msgkey,path+nodeID,hops)
               }
               else {network(routenode)! Msg(j,initnode,msgkey,path+nodeID+"+",hops+1,network)}               

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
    case _=>{}
  } 

}
 val mymax=math.pow(2,bits).toLong
class Master(numNodes: Int, numRequests:Int) extends Actor with ActorLogging {
    var count = 0
    var totalhops=0
     var NetWork = HashMap.empty[String, ActorRef] 
   // var mymap = HashMap.empty[String,Int] 
    var mymap = new TreeMap[Int,String]
    //val mymax=math.pow(2,bits).toLong
  

    override def preStart() {
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
        
    }
   def receive = {
       case finish(nodeindex,nodehop) =>{
               totalhops+=nodehop
               count+=1
               println(count)
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
