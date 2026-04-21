# Streamd BI - Fast R Exporter & Mapper
# This script uses data.table for ultra-fast mapping and CSV writing.

if (!require("data.table", quietly = TRUE)) {
  # install.packages("data.table", repos = "https://cran.rstudio.com/")
}
if (!require("jsonlite", quietly = TRUE)) {
  install.packages("jsonlite", repos = "https://cran.rstudio.com/")
}

library(data.table)
library(jsonlite)

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop("Usage: Rscript fast_export.R <input_temp_csv> <output_csv> [config_json_path]")
}

input_path <- args[1]
output_path <- args[2]
config_path <- if (length(args) >= 3) args[3] else NULL

tryCatch({
  message("Starting R script...")
  # 1. Read input data
  message(paste("Reading input file:", input_path))
  dt <- fread(input_path, encoding = "UTF-8")
  
  # 2. Apply CodeMap if config is provided
  if (!is.null(config_path) && file.exists(config_path)) {
    message(paste("Applying CodeMap from:", config_path))
    config <- fromJSON(config_path)
    acc_map <- config$code_map$account
    tax_map <- config$code_map$tax
    
    message(paste("Account map entries:", length(acc_map)))
    message(paste("Tax map entries:", length(tax_map)))
    
    # OPTIMIZED: Unique-value mapping strategy
    map_col_fast <- function(vals, error_fields, type) {
      if (length(vals) == 0) return(vals)
      
      current_map <- if (type == "account") acc_map else tax_map
      if (is.null(current_map) || length(current_map) == 0) return(vals)
      
      # Use liberal matching for keywords (handling potential encoding variations)
      # We look for ANY part of the common labels
      is_match <- if(type == "account") {
        grepl("科目|account|品目", error_fields, ignore.case = TRUE)
      } else {
        grepl("税区分|tax|税率", error_fields, ignore.case = TRUE)
      }
      
      match_count <- sum(is_match, na.rm = TRUE)
      message(paste("  - Pass", type, ":", match_count, "rows matched keywords"))
      
      if (match_count == 0) return(vals)
      
      # Get unique values that actually need mapping
      vals_to_map <- as.character(vals[is_match])
      unique_vals <- unique(vals_to_map)
      # Filter out noise
      unique_vals <- unique_vals[!is.na(unique_vals) & unique_vals != "" & unique_vals != "nan"]
      
      if (length(unique_vals) == 0) {
          message("    (No valid non-empty values to map)")
          return(vals)
      }
      
      # Map unique values once
      mapped_unique <- sapply(unique_vals, function(val) {
        # Handle multiple values (comma-sep)
        parts <- trimws(unlist(strsplit(val, ",")))
        mapped_parts <- sapply(parts, function(p) {
          # Direct lookup
          res <- current_map[[p]]
          if (is.null(res) || is.na(res)) p else res
        })
        paste(mapped_parts, collapse = ", ")
      })
      
      # Final vectorized replacement
      v_char <- as.character(vals)
      matches_orig_idx <- which(is_match)
      
      # Only replace those that are in our mapped_unique set
      to_replace <- v_char[matches_orig_idx]
      has_map <- to_replace %in% names(mapped_unique)
      
      if (any(has_map)) {
          v_char[matches_orig_idx[has_map]] <- mapped_unique[to_replace[has_map]]
          message(paste("    Applied mapping to", sum(has_map), "rows"))
      }
      
      return(v_char)
    }
    
    if ("初期値" %in% names(dt) && "修正後" %in% names(dt) && "エラー項目" %in% names(dt)) {
      message("Processing columns (Deep Match)...")
      
      # Force UTF-8 and character mode for columns
      v1 <- as.character(dt[["初期値"]])
      v2 <- as.character(dt[["修正後"]])
      ef <- as.character(dt[["エラー項目"]])
      
      # Pass 1: Account
      v1 <- map_col_fast(v1, ef, "account")
      v2 <- map_col_fast(v2, ef, "account")
      
      # Pass 2: Tax
      v1 <- map_col_fast(v1, ef, "tax")
      v2 <- map_col_fast(v2, ef, "tax")
      
      # Update columns directly
      dt[["初期値"]] <- v1
      dt[["修正後"]] <- v2
      
      message("Mapping process complete.")
    }
  }
  
  # 3. Write with BOM (ultra fast)
  message(paste("Writing output to:", output_path))
  fwrite(dt, output_path, bom = TRUE, nThread = parallel::detectCores())
  cat("DONE_SUCCESS\n")
  
}, error = function(e) {
  message(paste("Error:", e$message))
  quit(status = 1)
})
