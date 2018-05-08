####################################################################################

##

rm(list=ls())

##����׫д

#distance2<-function(lon_1,lon_2,lat_1,lat_2){
#  lon_1<-(lon_1/180)*pi
#  lon_2<-(lon_2/180)*pi
#  lat_1<-(lat_1/180)*pi
#  lat_2<-(lat_2/180)*pi
#  delta<-2*asin(sqrt(sin((lat_2-lat_1)/2)^2+cos(lat_1)*cos(lat_2)*(sin((lon_2-lon_1)/2)^2)))
#  return(6371*delta)
#}

velocityCal<-function(lon_1,lon_2,lat_1,lat_2,time_1,time_2){
  lon_1<-(lon_1/180)*pi
  lon_2<-(lon_2/180)*pi
  lat_1<-(lat_1/180)*pi
  lat_2<-(lat_2/180)*pi
  deltadegree<-2*asin(sqrt(sin((lat_2-lat_1)/2)^2+cos(lat_1)*cos(lat_2)*(sin((lon_2-lon_1)/2)^2)))
  deltatime<-time_2-time_1
  return(list(v=6371*deltadegree/(deltatime/3600),deltime=deltatime))
}

####################################################################################

##���ݴ���

#load the required packages

require(data.table)

#load the data from txt file 

carlist<-fread('C:\\Users/Administrator/Desktop/carid.csv')
carlist<-as.data.frame(carlist)
for(i in 1:nrow(carlist)){
  
  str_print<-paste('start of the preprocessing for the ',as.character(i),'th car - ',carlist[i,],'...',sep='')
  print(str_print)
  file_name<-paste('C:\\Users/Administrator/Desktop/gpsdata/data/',carlist[i,],'.txt',sep='')
  data<-fread(file_name)
  setnames(data,c('carid','time','mile','speed','lon','lat','orderid'))
  
  #processing the times and dates
  
  data[,`:=`(mile=NULL,speed=NULL)][,c('date','time'):=IDateTime(strptime(time,'%Y-%m-%d %H:%M:%S'))][,`:=`(time=as.numeric(difftime(date,'2010-01-01',units='secs'))+as.numeric(time),date=NULL)]
  
  #processing other data
  
  #��¼ԭʼ����������ȥ���ظ�����
  
  data.num_og<-nrow(data)
  setkey(data,orderid,time)
  data<-data[unique(data[,list(orderid,time)]),mult='first']
  
  #��¼���ظ���������find the first valid lonlat pair in each subset then delete data before it
  
  data.num_urep<-nrow(data)
  setkey(data,orderid,time)
  
  #ɾ����������Ƚ����صĶ�����Ҳ���ǵ�1�����ݲ��ں����з�Χ�ڵĶ�����ȫɾ(�ܹ�Ҳ��250������������)
  
  data<-data[data[,head(.SD,1),by=orderid][lon>=118&lon<=121&lat>=29&lat<=31][,list(unique(orderid))]]
  
  #����ÿ��ǰ�����ڵ����ݶԵ��ٶ�
  
  setkey(data,orderid,time)
  data[,`:=`(ind_1=seq(1,.N,by=1),ind_2=seq(0,.N-1,by=1)),by=orderid]
  setkey(data,orderid,ind_1,ind_2)
  data<-data[data[,list(carid,time,lon,lat,orderid,ind_2),keyby=.(orderid,ind_2)]][,c('v','deltime'):=velocityCal(lon,i.lon,lat,i.lat,time,i.time)]
  data<-data[,list(carid=i.carid,orderid=i.orderid,time=i.time,lon=i.lon,lat=i.lat,v,deltime)]
  
  #��¼��γ����û�����������������ʼ���ڳ��ٵ�������ϴ����
  
  data.num_actlonlat<-nrow(data)
  count=0
  while(1){
    
    #�ж��Ƿ����쳣�ٶȵ㣬��û��������ѭ��
    
    if(nrow(data[v>200])==0) break
    print(nrow(data[v>200]))
    
    #ɾȥÿ�����׸��ٶȳ����쳣�ĵ�
    
    setkey(data,orderid,time)
    temp<-data[v>150,min(time),by=orderid]
    setnames(temp,'V1','time')
    setkey(temp,orderid,time)
    #print(as.data.frame(data[temp])[,c('lon','lat')])
    data<-data[!temp]
    
    #��ѭ�������ѳ���100�Σ���ֱ��ɾ�������쳣�ٶȵ� --- ����취���ã��������ɾ
    
    #if(count>100) data<-data[v<=120]
    
    #�ټ���ÿ��ǰ�����ڵ����ݶԵ��ٶ�
    
    setkey(data,orderid,time)
    data[,`:=`(ind_1=seq(1,.N,by=1),ind_2=seq(0,.N-1,by=1)),by=orderid]
    setkey(data,orderid,ind_1,ind_2)
    data<-data[data[,list(carid,time,lon,lat,orderid,ind_2),keyby=.(orderid,ind_2)]][,c('v','deltime'):=velocityCal(lon,i.lon,lat,i.lat,time,i.time)]
    data<-data[,list(carid=i.carid,orderid=i.orderid,time=i.time,lon=i.lon,lat=i.lat,v,deltime)]
    
    #��ӡָʾ
    
    count=count+1
    
  }
  
  #����ÿ��ǰ�����ڵ����ݶԵļ��ٶ�
  
  setkey(data,orderid,time)
  data[,`:=`(ind_1=seq(1,.N,by=1),ind_2=seq(0,.N-1,by=1)),by=orderid]
  setkey(data,orderid,ind_1,ind_2)
  data<-data[data[,list(carid,time,lon,lat,v,orderid,deltime,ind_2),keyby=.(orderid,ind_2)]][,acct:=abs(i.v-v)/(3.6*0.5*(deltime+i.deltime))]
  data<-data[,list(carid=i.carid,orderid=i.orderid,time=i.time,lon=i.lon,lat=i.lat,deltime=i.deltime,v=i.v,acct)]
  
  #��¼��ʱ�ٶ������������������ʼ���ڼ��ٶȵ�������ϴ����
  
  data.num_regv<-nrow(data)
  count=0
  
  while(1){
    
    #�ж��Ƿ����쳣�ٶȵ㣬��û��������ѭ��
    
    if(nrow(data[acct>10])==0) break
    print(nrow(data[acct>10]))
    
    #ɾȥÿ�����׸����ٶȳ����쳣�ĵ�
    
    setkey(data,orderid,time)
    temp<-data[acct>10,min(time),by=orderid]
    setnames(temp,'V1','time')
    setkey(temp,orderid,time)
    data<-data[!temp]
    
    #�ټ���ÿ��ǰ�����ڵ����ݶԵ��ٶ�
    
    setkey(data,orderid,time)
    data[,`:=`(ind_1=seq(1,.N,by=1),ind_2=seq(0,.N-1,by=1)),by=orderid]
    setkey(data,orderid,ind_1,ind_2)
    data<-data[data[,list(carid,time,lon,lat,orderid,ind_2),keyby=.(orderid,ind_2)]][,c('v','deltime'):=velocityCal(lon,i.lon,lat,i.lat,time,i.time)]
    data<-data[,list(carid=i.carid,orderid=i.orderid,time=i.time,lon=i.lon,lat=i.lat,v,deltime)]
    
    #�ټ���ÿ��ǰ�����ڵ����ݶԵļ��ٶ�
    
    setkey(data,orderid,time)
    data[,`:=`(ind_1=seq(1,.N,by=1),ind_2=seq(0,.N-1,by=1)),by=orderid]
    setkey(data,orderid,ind_1,ind_2)
    data<-data[data[,list(carid,time,lon,lat,v,orderid,deltime,ind_2),keyby=.(orderid,ind_2)]][,acct:=abs(i.v-v)/(3.6*0.5*(deltime+i.deltime))]
    data<-data[,list(carid=i.carid,orderid=i.orderid,time=i.time,lon=i.lon,lat=i.lat,deltime=i.deltime,v=i.v,acct)]
    
    #��ӡָʾ
    
    count=count+1
    
  }
  
  #��¼���յ�������
  
  data.num_valid<-nrow(data)
  file_name<-paste('D:\\gpsdata/cleaning/',carlist[i,],'.txt',sep='')
  write.table(data,file=file_name,quote=FALSE,sep='\t',na='NA',col.names=F,append=F)
  file_name<-paste('D:\\gpsdata/datanum/',carlist[i,],'.csv',sep='')
  write.table(data.frame(data.num_og,data.num_urep,data.num_actlonlat,data.num_regv,data.num_valid),file=file_name,row.names=F,col.names=F,append=T,quote=FALSE)
  
  #�ͷ��ڴ�ռ�
  
  rm(list=c('data'))
  gc()
  
  #��ӡָʾ
  
  str_print<-paste('end of the preprocessing for the ',as.character(i),'th car - ',carlist[i,],sep='')
  print(str_print)
  rm(str_print)
  
  #END OF PREPROCESSING
}