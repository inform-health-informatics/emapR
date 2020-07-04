#' @title utils
#'
#' @description
#' utility functions for working with star
#'
#' @import data.table
#' @importFrom lubridate dhours
#'
#' @param ctn database connection
#' @param target_schema target schema
#' @param table_name table name
#' @param returnDT return as data.table instead of data.frame
#'



#' @export
select_star_from <- function(ctn, target_schema, table_name, returnDT=TRUE){
  'return table from database as data.table'
  table_path <- DBI::Id(schema=target_schema, table=table_name)
  dt <- DBI::dbReadTable(ctn, table_path)
  if (returnDT) setDT(dt)
  return(dt)
}

round_any <- function(x, accuracy = 1) {
  round(x / accuracy) * accuracy
}
