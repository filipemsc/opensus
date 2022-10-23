library(googleCloudStorageR)

gcs_auth("gcp-auth-dev.json")

gcs_global_bucket("opensus-dev")

download_upload_sia <- function(sigla_sia, 
                                  link_ftp = "ftp://ftp.datasus.gov.br/dissemin/publicos/SIASUS/200801_/Dados/",
                                  projectId = "opensus-dev"){
  
  path <- tolower(paste0("br_ms_sia/", sigla_sia,"/"))
  path_data <- tolower(paste0("br_ms_sia/", sigla_sia,"/data/"))
  
  ftp_files <- RCurl::getURLContent(link_ftp) |>
    strsplit("\r*\n") |>
    unlist()
  
  ftp_files <- gsub(" ", "\n", ftp_files) |>
    data.frame() |>
    tidyr::separate(col=1,
                    into=c("date","hour","size", "name"),
                    sep="\\\n{1,}") |>
    tidyr::separate(col=name, 
                    into=c("name","format"),
                    sep="\\.")
  
  ftp_files <- ftp_files[grep(paste0("^", sigla_sia), ftp_files$name),]
  
  # Check new files
  ftp_files$size <- as.numeric(ftp_files$size)
  ftp_files$date <- as.Date(ftp_files$date,format = "%m-%d-%y")
  
  ftp_files_names <- grep(sigla_sia, ftp_files$name, value=TRUE)
  
  gcs_files_names <- gcs_list_objects(prefix=path_data)$name
  gcs_files_names <- gsub("\\.parquet", "",gsub(path_data,"",gcs_files_names))
  
  files_queue <- ftp_files_names[!ftp_files_names %in% gcs_files_names]
  
  # Check if files needs update
  gcs_files <- gcs_list_objects(prefix=path_data)
  

  if(length(gcs_files)!=0){
    
    gcs_files$name <- gcs_files$name|> 
      fs::path_file() |>
      gsub(pattern = ".parquet", replacement="")
      
    
    ftp_files$updated_ftp <- paste(ftp_files$date, ftp_files$hour) |> as.POSIXct(tz = "America/Sao_Paulo")
    
    gcs_ftp_merge <- merge(gcs_files, ftp_files, by ="name")
    
    gcs_ftp_merge$need_update <- gcs_ftp_merge$updated_ftp >= gcs_ftp_merge$updated
    
    need_update <- gcs_ftp_merge[gcs_ftp_merge$need_update == TRUE,]$name
    
    files_queue <- c(files_queue, need_update)
  
  }
  
  if(length(files_queue)!=0){
    
    links_files_queue <- paste0(link_ftp, files_queue, ".dbc") 
    
    parse_sinan <- function(link){
      
      name_data <- gsub(".dbc", ".parquet",fs::path_file(link))
      name_schema <- paste0(gsub(".dbc", "",fs::path_file(link)),"_schema.csv")
      
      temp <- tempfile()
      download.file(link, temp, mode = "wb")
      partial <- read.dbc::read.dbc(temp)
      partial <- lapply(partial, as.character) |> data.frame()
      arrow::write_parquet(partial,name_data)
      
      schema <- data.frame(columns = arrow::open_dataset(name_data)$schema$names)
      schema <- utils::write.csv(schema, name_schema, row.names=FALSE)
      
      gcs_upload(name_data, bucket = "opensus-dev",
                 predefinedAcl = "bucketLevel",
                 name = paste0(path,"data/",name_data))
      
      gcs_upload(name_schema, 
                 bucket = "opensus-dev",
                 predefinedAcl = "bucketLevel",
                 name = paste0(path,"schema/",name_schema))
      
      file.remove(temp)
      fs::file_delete(name_data)
      fs::file_delete(name_schema)
      rm(schema)
      rm(partial)
      
      return(name_data)
      
    }
    
    lapply(links_files_queue, parse_sinan)
  }
  
}

check <- download_upload_sia("PASE")

if(length(unlist(check))==0){print("Nenhum arquivo atualizado.")}
if(length(unlist(check))> 0){print(paste("Arquivo atualizado:", unlist(check)))}
