## Assumes loaded from 020_LoadCurate_All

sapply(
  list.files(dir_etl_cur_fact)
  ,function(x){
    dir <- paste0(c(dir_etl_cur_fact,x),collapse="/")
    print(paste0("Executing dimension load for ETL rule ",x))
    source(dir)
  }
)


