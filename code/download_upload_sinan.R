library(googleCloudStorageR)

gcs_auth("gcp-auth-dev.json")

download_upload_sinan <- function(sigla_doenca, 
                                  link_sinan = "ftp://ftp.datasus.gov.br/dissemin/publicos/SINAN/DADOS/FINAIS/",
                                  projectId = "opensus-dev"){

bucket_name <- tolower(paste0("sinan_", sigla_doenca))
bucket_list <- gcs_list_buckets(projectId = projectId)$name

if(!(bucket_name %in% bucket_list)){
  gcs_create_bucket(name = bucket_name,
                    projectId = projectId)
}

ftp_files <- RCurl::getURLContent(link_sinan) |>
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

ftp_files$size <- as.numeric(ftp_files$size)
ftp_files$date <- as.Date(ftp_files$date,format = "%m-%d-%y")

ftp_files_names <- grep(sigla_doenca, ftp_files$name, value=TRUE)

gcs_files_names <- gcs_list_objects(bucket = bucket_name)$name 
gcs_files_names <- gsub(".parquet", "",gcs_files_names)

files_queue <- ftp_files_names[!ftp_files_names %in% gcs_files_names]

if(length(files_queue)!=0){
  
  links_files_queue <- paste0(link_sinan, files_queue, ".dbc")
  
  parse_sinan <- function(link){
    temp <- tempfile()
    name <- gsub(".dbc", ".parquet",fs::path_file(link))
    download.file(link, temp, mode = "wb")
    partial <- read.dbc::read.dbc(temp)
    arrow::write_parquet(partial,name)
    gcs_upload(name, bucket = bucket_name,
               predefinedAcl = "bucketLevel",
               name = name)
    file.remove(temp)
    fs::file_delete(name)
  }
  
  lapply(links_files_queue, parse_sinan)
}

}
