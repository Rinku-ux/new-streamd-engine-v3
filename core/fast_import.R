# Streamd BI - Fast R Importer
# This script handles bulk CSV reading and CodeMap mapping during the import phase.

if (!require("data.table", quietly = TRUE)) {}
if (!require("jsonlite", quietly = TRUE)) {
  install.packages("jsonlite", repos = "https://cran.rstudio.com/")
}

library(data.table)
library(jsonlite)

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 3) {
  stop("Usage: Rscript fast_import.R <input_paths_comma_sep> <output_csv> <config_json_path> [is_drilldown]")
}

input_paths <- trimws(unlist(strsplit(args[1], ",")))
output_path <- args[2]
config_path <- args[3]
is_drilldown <- if (length(args) >= 4) as.logical(args[4]) else FALSE

tryCatch({
  message("Starting Fast Import Bridge...")
  
  # 1. Load CodeMap
  acc_map <- NULL
  tax_map <- NULL
  if (file.exists(config_path)) {
    config <- fromJSON(config_path)
    acc_map <- config$code_map$account
    tax_map <- config$code_map$tax
  }
  
  # 2. Read and Combine all input CSVs
  message(paste("Reading", length(input_paths), "files..."))
  dt_list <- lapply(input_paths, function(p) {
    if (!file.exists(p)) return(NULL)
    fread(p, encoding = "UTF-8", colClasses = "character")
  })
  dt <- rbindlist(dt_list, fill = TRUE)
  
  if (nrow(dt) == 0) {
    stop("No data found in input files.")
  }

  # 3. Apply Mapping if needed
  # We reuse the same unique-value matching logic for speed
  map_col_fast <- function(vals, error_fields, type) {
    if (length(vals) == 0) return(vals)
    current_map <- if (type == "account") acc_map else tax_map
    if (is.null(current_map) || length(current_map) == 0) return(vals)
    
    # Identify matches
    is_match <- if(type == "account") grepl("科目|account", error_fields, ignore.case = TRUE)
                else grepl("税区分|tax", error_fields, ignore.case = TRUE)
    
    if (!any(is_match, na.rm=TRUE)) return(vals)
    
    vals_to_map <- as.character(vals[is_match])
    u_vals <- unique(vals_to_map)
    u_vals <- u_vals[!is_na(u_vals) & u_vals != "" & u_vals != "nan"]
    
    if (length(u_vals) == 0) return(vals)
    
    mapped_u <- sapply(u_vals, function(v) {
      parts <- trimws(unlist(strsplit(v, ",")))
      mapped_parts <- sapply(parts, function(p) {
        res <- current_map[[p]]
        if (is.null(res) || is.na(res)) p else res
      })
      paste(mapped_parts, collapse = ", ")
    })
    
    v_char <- as.character(vals)
    idx <- which(is_match)
    targets <- v_char[idx]
    has_m <- targets %in% names(mapped_u)
    if (any(has_m)) {
      v_char[idx[has_m]] <- mapped_u[targets[has_m]]
    }
    return(v_char)
  }

  is_na <- function(x) is.na(x) | x == "NA"

  col_initial <- if (is_drilldown) "initial_value" else "初期値"
  col_latest  <- if (is_drilldown) "latest_value" else "修正後"
  col_error   <- if (is_drilldown) "error_field" else "エラー項目"

  if (col_initial %in% names(dt) && col_error %in% names(dt)) {
    message("Applying Bulk Mapping...")
    # Account
    dt[[col_initial]] <- map_col_fast(dt[[col_initial]], dt[[col_error]], "account")
    if (col_latest %in% names(dt)) dt[[col_latest]] <- map_col_fast(dt[[col_latest]], dt[[col_error]], "account")
    
    # Tax
    dt[[col_initial]] <- map_col_fast(dt[[col_initial]], dt[[col_error]], "tax")
    if (col_latest %in% names(dt)) dt[[col_latest]] <- map_col_fast(dt[[col_latest]], dt[[col_error]], "tax")
  }

  # 4. Write cleaned CSV for DuckDB
  message(paste("Writing optimized result to:", output_path))
  fwrite(dt, output_path, bom = TRUE, nThread = parallel::detectCores())
  cat("DONE_SUCCESS\n")

}, error = function(e) {
  message(paste("Import Error:", e$message))
  quit(status = 1)
})
