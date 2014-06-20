mapper <- function(pdf){
  pdf = as.data.frame(table(pdf$Month))
  colnames(pdf) <- c("Month", "Freq")
  pdf
}
hasher <- function(pdf, nr){
  pdf$rbucket <- 1
  pdf
}
reducer <- function(pdf){
  pdf = aggregate(Freq ~ Month, data=pdf, sum)
  pdf
}

# longer solution

a <- read.csv.ffdf(file="big.csv", header=TRUE, VERBOSE=TRUE, first.rows=1000000, next.rows=1000000, colClasses=NA)
totalrows = dim(a)[1]
row.size = as.integer(object.size(a[1:10000,])) / 10000
#(object.size(a[1:100,]) / 100)* (dim(a)[1])
block.size = 200000000
rows.block = ceiling(block.size/row.size)
nmaps = floor(totalrows/rows.block)
nr = 1
nmaps = 10
#nmaps = number of maps - 1

for(i in (0:nmaps)){
  if(i==nmaps){
    #df = a[(i*rows.block+1) : ((i+1)*rows.block),]
    df = a[(i*rows.block+1) : totalrows,]
  }
  else{
    df = a[(i*rows.block+1) : ((i+1)*rows.block),]
  }
  #stop()
  ##parameter a, block.size, nr, hasher,mapper, reducer
  # can be parameters first.rows, next.rows
  df = hasher(mapper(df), nr)
  mappedlist = split(df, df$rbucket)
  
  #bucket names or rbucket field should be 1 to nr
  rbucketnames = names(mappedlist)
  for(j in 1:length(mappedlist)){
    write.csv(mappedlist[[j]], paste0("R", rbucketnames[j],"M",i+1,".csv"))
  }
  #lapply(mappedlist, FUN = function(y){write.csv(y, "M1r1.csv")})
  rm(df)
}

for(i in 1:nr){
  rbucketname = as.character(i)
  for(j in 1:nmaps+1){
    fname = paste0("R", rbucketname, "M", j,".csv")
    
    if(file.exists(fname)){
      if(exists("rdf")){
        rdf = rbind(rdf, read.csv(fname))
      }
      else{
        rdf = read.csv(fname)
      }
    }
  }
  if(exists("rdf")){
    finalrdf = reducer(rdf)
    write.csv(finalrdf, paste0("R",rbucketname,".csv"))
    rm(rdf)
    rm(finalrdf)
  }  
}




#for(i in (0:4)){print(i*rows.block+1);print((i+1)*rows.block);}


