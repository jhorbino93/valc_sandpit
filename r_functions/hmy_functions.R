library(httr)
library(jsonlite)

fn_bnToReal <- function(x){
  x/1e18
}

fn_unixToTime <- function(x,origin="1970-01-01",tz="UTC"){
  as.POSIXct(x,origin=origin,tz=tz)
}

fn_getHermesPid <- function(address,token1,token2){
  
}

fn_hmyv2_getBlock <- function(rpc="https://rpc.hermesdefi.io",id="1",jsonrpc="2.0"){
  res <- POST(
    url = rpc
    ,body = jsonlite::toJSON(
      list(
        id = id
        ,jsonrpc = jsonrpc
        ,method = "hmyv2_blockNumber"
        ,params = list()
      )
      ,auto_unbox = T
      ,pretty = T
    )
    ,httr::content_type('application/json')
  )
  return(res)
}

fn_hmyv2_getBalance <- function(address,rpc="https://rpc.hermesdefi.io",id="1",jsonrpc="2.0"){
  body = jsonlite::toJSON(
    list(
      id       = id
      ,jsonrpc = jsonrpc
      ,method  = "hmyv2_getBalance"
      ,params  = list(address)
    )
    ,auto_unbox=T
    ,pretty=T
  )   
  res <- POST(url = rpc,body=body,httr::content_type("application/json"))
  return(res)
}

fn_hmyv2_getBalanceByBlockNumber <- function(address,block = content(fn_hmyv2_getBlock(rpc=rpc_url_base))$result,offset=0,rpc="https://rpc.hermesdefi.io",id="1",jsonrpc="2.0"){
  body = jsonlite::toJSON(
    list(
      id       = id
      ,jsonrpc = jsonrpc
      ,method  = "hmyv2_getBalanceByBlockNumber"
      ,params  = list(address,block-offset)
    )
    ,auto_unbox=T
    ,pretty=T
  )   
  res <- POST(url = rpc,body=body,httr::content_type("application/json"))
  return(res)
}

fn_hmyv2_call <- function(
  token_address
  # ,ABI
  ,data
  ,rpc="https://rpc.hermesdefi.io"
  ,block=NULL
  ,id="1"
  ,jsonrpc="2.0"
){
  ## Assumes ABI input is first 4 bytes (or 8 characters in hex form)
 
  # my_address2 = sub("..","",my_address)
  # pad = paste0(rep("0",24),collapse="")
  # data = paste0("0x",ABI,pad,my_address2)
  
  if(is.null(block)){
    params <-
      list(
        list(
          to = token_address
          ,data = data
        )
        ,"latest"
      )
  }
  else {
    params <-
      list(
        list(
          to = token_address
          ,data = data
        )
        ,block
      )
  }
    
  body <- jsonlite::toJSON(
    list(
      id = id
      ,jsonrpc = jsonrpc
      ,method  = "hmyv2_call"
      ,params  = params
    )
    ,auto_unbox=T
    ,pretty=T
  )
  
  res <- POST(url=rpc,body=body,httr::content_type("application/json"))
  return(res)
}

fn_hmyv2_call_emissionPerBlock <- function(
  masterchef_address
  ,data = "0x4198709a"
  ,rpc="https://rpc.hermesdefi.io"
  ,block=NULL
  ,id="1"
  ,jsonrpc="2.0"
){
  return(fn_hmyv2_call(token=masterchef_address,data=data,rpc=rpc,block=block))
}

fn_hmyv2_call_EmissionToken <- function(
  .masterchef_address=masterchef_address
  ,.data = "0xfc0c546a"
  ,.rpc="https://a.api.s0.t.hmny.io/"
  ,.block=NULL
  ,.id="1"
  ,.jsonrpc="2.0"
){
  
  
  res <- fn_hmyv2_call(token_address=.masterchef_address,data=.data,rpc=.rpc,id=.id,jsonrpc=.jsonrpc)
  res <- content(res)$result
  return(paste0("0x",substr(res,27,nchar(res))))
}

fn_hmyv2_call_totalAllocPoints <- function(
  .masterchef_address=masterchef_address
  ,.data = "0x17caf6f1"
  ,.rpc="https://a.api.s0.t.hmny.io/"
  ,.block=NULL
  ,.id="1"
  ,.jsonrpc="2.0"
){
  res <- fn_hmyv2_call(token_address=.masterchef_address,data=.data,rpc=.rpc,id=.id,jsonrpc=.jsonrpc)
  res <- content(res)$result
  return(hex_to_dec(res))
}


fn_hmyv2_call_poolInfo <- function(
  masterchef_address
  ,data = NULL
  ,pid = NULL
  ,rpc="https://rpc.hermesdefi.io"
  ,block=NULL
  ,id="1"
  ,jsonrpc="2.0"
){
  if(is.null(data)){
    hex_pid = dec_to_hex(pid)
    data = paste0("0x","1526fe27",gsub(" ","0",sprintf("%064s",substr(hex_pid,3,nchar(hex_pid)))))
  }
  
  return(fn_hmyv2_call(token=masterchef_address,data=data,rpc=rpc,block=block))
}

fn_hmyv2_call_totalSupply <- function(
  address
  ,data="0x18160ddd"
  ,rpc="https://a.api.s0.t.hmny.io/"
  ,block=NULL
  ,id="1"
  ,jsonrpc="2.0"
){
  res <- fn_hmyv2_call(token=address,data=data,rpc=rpc,block=block)
  return(as.numeric(content(res)$result)/1e18)
}

fn_poolInfo_allocPoints <- function(x){
  ## Assumes result structure in line with address/uint256/uint256/uint16/uint256
  hex_to_dec(paste0("0x",substr(x,67,130)))
}

fn_hmyv2_call_balanceOf <- function(
    token_address
    ,my_address
    ,rpc="https://rpc.hermesdefi.io"
    ,block=NULL
    ,id="1"
    ,jsonrpc="2.0"
    ,ABI="70a08231"
  ){
    ## Default ABI is hermes balanceOf first 4 bytes
    my_address2 = sub("..","",my_address)
    pad = paste0(rep("0",24),collapse="")
    data = paste0("0x",ABI,pad,my_address2)
  
    res = fn_hmyv2_call(token_address=token_address,rpc=rpc,block=block,id=id,jsonrpc=jsonrpc,data=data)
    
    return(as.numeric(content(res)$result)/1e18)
  }

fn_hmyv2_getBlockByNumber <- function(block,fullTx=T,inclTx=T,withSigners=F,rpc="https://rpc.hermesdefi.io",id="1",jsonrpc="2.0"){
  
  list_additional <- list()
  if(fullTx){list_additional[["fullTx"]]=T}
  if(inclTx){list_additional[["inclTx"]]=T}
  if(withSigners){list_additional[["withSigners"]]=T}
  
  params <- 
    list(
      block
      ,list_additional
    )
  body <- jsonlite::toJSON(
    list(
      id       = id
      ,jsonrpc = jsonrpc
      ,method  = "hmyv2_getBlockByNumber"
      ,params  = params
    )
    ,auto_unbox=T
    ,pretty=T
  )
  res <- POST(url=rpc,body=body,httr::content_type("application/json"))
  return(res)
}


fn_getClosestBlock <- function(block=content(fn_hmyv2_getBlock(rpc="https://a.api.s0.t.hmny.io/"))$result,target,attempts=3,tol_seconds=6){
  startBlockTime = fn_unixToTime(content(fn_hmyv2_getBlockByNumber(block))$result$timestamp)
  
  n_sec = interval(target,startBlockTime) %/% seconds()
  n_block = floor(n_sec/2)
  
  diff_sec = tol_seconds+1
  attempt_count = 0
  attempt_block = block
  attempt_time  = startBlockTime
  list_res <- list()
  while(abs(diff_sec) >= tol_seconds & attempt_count <= attempts){
    attempt_count = attempt_count+1
    print(paste0("Attempt ",attempt_count))
    print(paste0("Target time ",target))
    print(paste0("Attempting from block ",attempt_block," of time ",attempt_time))
    
    
    n_sec = interval(target,attempt_time) %/% seconds()
    n_block = floor(n_sec/2)
    
    attempt_block = attempt_block-n_block
    attempt_time = fn_unixToTime(content(fn_hmyv2_getBlockByNumber(attempt_block))$result$timestamp)
    
    diff_sec = interval(target,attempt_time) %/% seconds()
    
    list_res[[attempt_count]] = 
      list(
        n_sec = n_sec
        ,n_block = n_block
        ,attempt_block = attempt_block
        ,attempt_time = attempt_time
        ,diff_sec = diff_sec
        ,target = target
      )
  }
  
  return(list_res[[length(list_res)]])
}







