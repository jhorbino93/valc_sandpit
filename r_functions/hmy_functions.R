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

fn_hmyv2_getBlock <- function(rpc="https://a.api.s0.t.hmny.io/",id="1",jsonrpc="2.0"){
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

fn_hmyv2_getBalance <- function(address,rpc="https://a.api.s0.t.hmny.io/",id="1",jsonrpc="2.0"){
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

fn_hmyv2_getCode <- function(address,block=NULL,rpc="https://a.api.s0.t.hmny.io/",id="1",jsonrpc="2.0"){
  body = jsonlite::toJSON(
    list(
      id       = id
      ,jsonrpc = jsonrpc
      ,method  = "hmyv2_getCode"
      ,params  = list(address,block)
    )
    ,auto_unbox=T
    ,pretty=T
  )   
  res <- POST(url = rpc,body=body,httr::content_type("application/json"))
  return(res)
}

fn_hmyv2_getBalanceByBlockNumber <- function(address,block = content(fn_hmyv2_getBlock(rpc=rpc_url_base))$result,offset=0,rpc="https://a.api.s0.t.hmny.io/",id="1",jsonrpc="2.0"){
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
  ,rpc="https://a.api.s0.t.hmny.io/"
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
  ,rpc="https://a.api.s0.t.hmny.io/"
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
  ,rpc="https://a.api.s0.t.hmny.io/"
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
    ,rpc="https://a.api.s0.t.hmny.io/"
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

fn_hmyv2_getBlockByNumber <- function(block,fullTx=T,inclTx=T,withSigners=F,rpc="https://a.api.s0.t.hmny.io/",id="1",jsonrpc="2.0"){
  
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
  avg_block_time = 2 # Default start at 2
  list_res <- list()
  while(abs(diff_sec) >= tol_seconds & attempt_count <= attempts){
    attempt_count = attempt_count+1
    print(paste0("Attempt ",attempt_count))
    print(paste0("Target time ",target))
    print(paste0("Attempting from block ",attempt_block," of time ",attempt_time))
    
    n_sec = interval(target,attempt_time) %/% seconds()
    n_block = floor(n_sec/avg_block_time)
    
    prev_block = attempt_block
    prev_time = attempt_time
    
    attempt_block = attempt_block-n_block
    attempt_time = fn_unixToTime(content(fn_hmyv2_getBlockByNumber(attempt_block))$result$timestamp)
    
    diff_sec = interval(target,attempt_time) %/% seconds()
    
    avg_block_time = abs(as.integer(difftime(attempt_time,prev_time,unit="secs"))/(attempt_block-prev_block))
    
    list_res[[attempt_count]] = 
      list(
        n_sec = n_sec
        ,n_block = n_block
        ,attempt_block = attempt_block
        ,attempt_time = attempt_time
        ,diff_sec = diff_sec
        ,target = target
        ,avg_block_time = avg_block_time
      )
  }
  
  return(list_res[[length(list_res)]])
}

fn_hmyv2_call_startBlock <- function(
  masterchef_address = "0x8c8dca27e450d7d93fa951e79ec354dce543629e"
  ,data = "0x48cd4cb1"
  ,block=NULL
  ,id="1"
  ,jsonrpc="2.0"
  ,rpc = "https://a.api.s0.t.hmny.io/"
){
  return(
    as.numeric(
      content(
        fn_hmyv2_call(token_address=masterchef_address,rpc=rpc,data=data,block=block,id=id,jsonrpc=jsonrpc)
      )$result
    )
  )
}

fn_getCodeStartBlock <- function(
  address
  ,.block=content(fn_hmyv2_getBlock(rpc="https://a.api.s0.t.hmny.io/"))$result
  ,attempts=5
){
  
  block = .block
  delta_block = floor(block/2)
  numCode = 0
  attempt_count = 0
  list_res = list()
  while(numCode == 0 & attempt_count <= attempts){
    attempt_count = attempt_count+1
    
    new_block = block - delta_block
    new_res = as.numeric(content(fn_hmyv2_getCode(address=address,block=new_block))$result)
    
    print(paste0("Attempt ",attempt_count))
    print(paste0("Attempting from block ",block," to block ",new_block))
    
    ## Get existing loop results + current loop result new res
    vct_code_found = c(do.call("c",lapply(list_res,function(x) x$new_res)),new_res)
    vct_code_found = !is.na(vct_code_found)
    
    code_found = sum(vct_code_found)>0
    list_code_found = c(list_res,list(list(prev_delta_block = delta_block, prev_block = block, new_block = new_block)))[vct_code_found]
    
    ## Get latest list entry that has opposite new_res status
    # if(attempt_count == 1 | !code_found){
    #   latest_prev_new_block = new_block
    # } else {
    #   new_res_status = !is.na(new_res)
    #   latest_prev_status = max(which(vct_code_found == !new_res_status))
    #   latest_prev = list_res[[latest_prev_status]]
    #   latest_prev_new_block = latest_prev$new_block
    # }
    
    
    ## Get existing min code block
    if(all(do.call("c",lapply(list_code_found,is.null)))){
      floor_block = 1
      ceiling_block = .block
    } else {
      min_code_found = 
        lapply(
          list_code_found
          ,function(x){
            if(x$prev_delta_block < 0){
              res = x$prev_block
            } else if (x$prev_delta_block >= 0){
              res = x$new_block
            }
          }
        )
      floor_block = min(do.call("c",min_code_found))
      
      max_code_found = 
        lapply(
          list_code_found
          ,function(x){
            if(x$prev_delta_block < 0){
              res = x$new_block
            } else if (x$prev_delta_block >= 0){
              res = x$prev_block
            }
          }
        )
      ceiling_block = max(do.call("c",max_code_found))
    }
    
    ## Use halving grid search technique to find closest block
    ## Ignore floor/ceiling limits
    if(is.na(new_res) & delta_block >= 0){
      # No code and direction down, then need to go up
      new_delta_block = floor((new_block-block)/2)  
    } else if (is.na(new_res) & delta_block < 0){
      # No code and direction up, then need to go up
      new_delta_block = floor((block-new_block)/2)
    } else if (!is.na(new_res) & delta_block < 0){
      # Found code and direction up, then need to go down
      new_delta_block = floor(new_block/2)
    } else if (!is.na(new_res) & delta_block >= 0){
      # Found code and direction down, then need to go down
      new_delta_block = floor((block-new_block)/2)
    }
    
    ## For floor/ceiling limits
    if(code_found & new_delta_block < 0){
      ## If new delta block would increase higher than max_code_found, then adj so it hits max_code_found
      new_delta_block = floor(new_delta_block - min(0,ceiling_block - (new_block - new_delta_block)))
      new_delta_block = floor(new_delta_block/2)
    } else if (code_found & new_delta_block >= 0){
      ## If new delta block would decrease lower than min_code_found, then adj so it hits min_code_found
      new_delta_block = floor(new_delta_block + min(0,(new_block - new_delta_block) - floor_block))
      new_delta_block = floor(new_delta_block/2)
    }
    
    list_res[[attempt_count]] =
      list(
        prev_delta_block = delta_block
        ,prev_block = block
        ,new_delta_block = new_delta_block
        ,new_block = new_block
        ,new_res = new_res
      )
    
    block = new_block
    delta_block = new_delta_block
  }
  
  ## If final try returns Code Block then get previous tried block
  ## If final try doesn't return code block then get last tried block
  if(!is.na(list_res[[length(list_res)]]$new_res)){
    res_block = list_res[[length(list_res)]]$prev_block
  } else {
    res_block = list_res[[length(list_res)]]$new_block
  }
  
  return(
    list(
      res = res_block
      ,attempts = list_res
    )
  )
}


fn_hmyv2_Call_startBlock <- function(
  address
  ,block=NULL
  ,rpc="https://a.api.s0.t.hmny.io/"  
  ,id="1"
  ,jsonrpc="2.0"
){
  as.numeric(
    content(
      fn_hmyv2_call(
        token_address=address
        ,data="0x48cd4cb1"
        ,rpc=rpc
        ,id=id
        ,jsonrpc=jsonrpc
      )
    )$result
  )
}


