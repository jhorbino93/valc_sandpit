rpc <- "https://a.api.s0.t.hmny.io/"
glbStartTime     <- with_tz(as_datetime("2021-07-01 00:00:00"),"UTC")

block <- content(fn_hmyv2_getBlock(rpc=rpc))$result
currentBlockTime <- fn_unixToTime(content(fn_hmyv2_getBlockByNumber(block))$result$timestamp)
vct_time <- seq(glbStartTime,currentBlockTime,by="hour")

library(foreach)
library(parallel)
parallelPackages=c("httr","jsonlite","ether","dplyr","lubridate")
parallelExport = c("fn_unixToTime","fn_hmyv2_getBlockByNumber","fn_getClosestBlock")

start.time <- Sys.time()
cores <- detectCores()
cl <- makeCluster(cores[1]-1)
on.exit(stopCluster(cl))
registerDoParallel(cl)

list_blocks <- foreach(
  x=vct_time
  ,.packages=parallelPackages
) %dopar% {
  
  target <- x
  print(target)
  
  fn_getClosestBlock(block,target,attempts=6)  
}
stopCluster(cl)
end.time <- Sys.time()
time.taken <- end.time - start.time
time.taken