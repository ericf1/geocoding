library(fst)
library(rjson)
library(photon)
library(stringr)
library(data.table)
library(haven)
library(reticulate)
path_to_python <- "/Library/Developer/CommandLineTools/usr/bin/python3"
use_python(path_to_python)

#SET YOUR WORKING DIRECTORY
setwd("/Users/human/Downloads/GeoCoding")

########
### INDIVIDUAL POPULATION: ALGORITHM

#### MUNI IDS
muni <-as.data.table(read_dta("muni_ids.dta"))

for(i in 1:1){
  row_to <- 1000000*i
  
  # MAKE SURE NOT TO EXTRACT MORE ROWS THAN THE LENGTH OF RAW DATA
  if (row_to>10000) {row_to = 10000}
  data <- read_fst(
    path = "family_id.fst",
    columns = NULL,
    from = 1000000*(i-1)+1,
    to = row_to ,
    as.data.table = TRUE,
    old_format = FALSE
  )
  
  
  ## MUNI IDS
  data <- muni[,list(codigo_do_municipio_no_bcbase,nome_do_municipio,sigla_da_unidade_federativa)][data,on=.(codigo_do_municipio_no_bcbase)]
  setnames(data,"sigla_da_unidade_federativa","uf")
  
  ## ADD FULL NAMES OF STATES
  data[,`:=`(state=fcase(
    uf=="DF","Distrito Federal"
    ,uf=="AM", "Amazonas"
    ,uf=="PA","Par?"
    ,uf=="SP", "S?o Paulo"
    ,uf=="RJ" ,"Rio de Janeiro"
    ,uf=="BA" ,"Bahia"
    ,uf=="PE" ,"Pernambuco"
    ,uf=="CE" ,"Cear?"
    ,uf=="PR" ,"Paran?" 
    ,uf=="RS" ,"Rio Grande do Sul"
    ,uf=="PB" ,"Para?ba"
    ,uf=="MG" ,"Minas Gerais" 
    ,uf=="AL" ,"Alagoas"
    ,uf=="MS" ,"Mato Grosso do Sul"
    ,uf=="SC" ,"Santa Catarina" 
    ,uf=="SE" ,"Sergipe"
    ,uf=="MA" ,"Maranh?o"
    ,uf=="ES" ,"Esp?rito Santo"
    ,uf=="RN" ,"Rio Grande do Norte"
    ,uf=="PI" ,"Piau?"
    ,uf=="GO" ,"Goi?s" 
    ,uf=="MT" ,"Mato Grosso" 
    ,uf=="AC" ,"Acre"  
    ,uf=="RO" ,"Rond?nia"
    ,uf=="EX" ,"EX"
    ,uf=="RR" ,"Roraima" 
    ,uf=="AP" ,"Amap?" 
    ,uf=="TO","Tocantins"
  ))]
  ## KEEP ONLY LETTERS AND NUMBERS
  data[,`:=`(addr=
               str_extract(
                 str_replace_all(
                   paste0(logradouro," "
                          ,numero,", "
                          ,codigo_cep,", "
                          ,nome_do_municipio,", "
                          ,state)
                   ,"([^[:alnum:]|[:space:]|,])+","")
                 ,"[[:alnum:]|[:space:]|,]+")
             ,addr2=
               str_extract(
                 str_replace_all(
                   paste0(logradouro," "
                          ,numero,", "
                          ,nome_do_municipio,", "
                          ,state)
                   ,"([^[:alnum:]|[:space:]|,])+","")
                 ,"[[:alnum:]|[:space:]|,]+")
  )]
  addr <- unique(data[,c("addr2")])
  gc()
  
  ## SEARCH BY INDIVIDUAL'S ADDRESS
  Sys.time()
  
  rows = 1
  max_row = nrow(addr)
  load_rows = 100
  # load_rows = 5000
  ## SEARCH BY COMPANY ADDRESS
  Sys.time()
  
  
  #################
  ### THE LOOP CAN BE OPTIMIZED: MERGE RAIS FIRST AND THEN EXPAND ROWS! this should be much faster
  #################
  
  ##2002 - 2015
  searches = data.frame(query=NULL)
  while (rows<max_row)  {
    ## RIGHT JOIN RAIS TO CONSORCIO
    if (load_rows+rows-1>max_row) {load_rows = max_row +1-rows}
    
    search <- addr[rows:(load_rows+rows-1),]$addr2
    search <- data.frame(query=search)
    print(search)
    searches <- rbind(searches,search)
    rows = rows + load_rows
  }
  write.csv(searches, ("searches"), row.names = FALSE)
  py_run_file("geocode.py")
  gc()
}
