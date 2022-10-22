library(googleCloudStorageR)

gcs_auth("gcp-auth-dev.json")

gcs_global_bucket("opensus-dev")

get_schema <- function(sigla_doenca){
  
  name_schema <- tolower(paste0("zzz_schema_",sigla_doenca,".parquet"))
  
  path_data <- tolower(paste0("br_ms_sia/", sigla_doenca,"/data/"))
  path_schema <- tolower(paste0("br_ms_sia/", sigla_doenca,"/schema/"))
  
  files_schema <- gcs_list_objects(prefix=path_schema)$name
  
  all_schemas <- lapply(files_schema, gcs_get_object)
  
  columns <- unique(do.call(rbind, all_schemas))$columns
  
  df <- data.frame(matrix(,ncol=length(columns)))
  df <- data.frame(lapply(df, as.character))
  names(df) <- columns
  
  arrow::write_parquet(df[0,],name_schema)
  
  gcs_upload(name_schema, bucket = "opensus-dev",
             predefinedAcl = "bucketLevel",
             name = paste0(path_data,name_schema))
  
  fs::file_delete(name_schema)
  
  return(sigla_doenca)
  
}

get_schema("PASE")