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
#' @export
round_any <- function(x, accuracy = 1) {
  'round things'
  round(x / accuracy) * accuracy
}

#' @export
udsDisconnect <- function() {
  'Disconnect from the database assuming the connection is called ctn'
  e <- globalenv()
  objs <- sapply(ls(e), as.character)
  assertthat::assert_that('ctn' %in% objs, msg= '!!! Cannot find connection named ctn in global environment')
  DBI::dbDisconnect(e$ctn)
}

#' @export
udsConnect <- function() {
  # Make connection to the UDS using environment variables
  # UDS_HOST, UDS_PWD and UDS_USER should be stored in .Renviron
  # .Renviron should be excluded using .gitignore
  assertthat::assert_that(nchar(Sys.getenv("UDS_HOST")) > 0,
                          msg='!!! No environment variable UDS_HOST found')
  assertthat::assert_that(nchar(Sys.getenv("UDS_PWD")) > 0,
                          msg='!!! No environment variable UDS_PWD found')
  assertthat::assert_that(nchar(Sys.getenv("UDS_USER")) > 0,
                          msg='!!! No environment variable UDS_USER found')
  rlang::inform('--- connecting to UDS database ...')
  ctn <- DBI::dbConnect(RPostgres::Postgres(),
                        host = Sys.getenv("UDS_HOST"),
                        port = 5432,
                        user = Sys.getenv("UDS_USER"),
                        password = Sys.getenv("UDS_PWD"),
                        dbname = "uds")
  rlang::inform('--- success: connected to UDS database: connection ctn placed in global environment')
  rlang::inform('--- please call udsDisconnect at the end of your session')
  # assign to global environment
  ctn <<- ctn
  return(TRUE)
}

#' @export
select_from <- function(ctn,
                        target_schema,
                        table_name,
                        cols=NULL,
                        returnDT=TRUE){
  'return table from database as data.table'
  if (is.null(cols)) {
    table_path <- DBI::Id(schema=target_schema, table=table_name)
    dt <- DBI::dbReadTable(ctn, table_path)
  } else {
    # Prepare query
    schema_table <- paste(target_schema, table_name, sep='.')
    field_string <- paste(cols, collapse=', ')
    query <- paste('SELECT', field_string, 'FROM', schema_table)
    dt <- DBI::dbGetQuery(ctn, query)
  }
  if (returnDT) return(setDT(dt)) else return(dt)
}

#' @export
omop_select_from <- function(ctn,
                             target_schema,
                             table_name,
                             cols=NULL,
                             concepts=NULL,
                             chunk_size = 1e4,
                             returnDT=TRUE){
  #' return a selection of columns from one of the OMOP tables
  # checks
  checkmate::assert_choice(table_name, c('observation', 'measurement'))
  assertthat::assert_that( DBI::dbIsValid(ctn) )
  where_clause <- ''

  if (is.null(cols)) {
    if (table_name  == 'observation') {
      fields <- c(
                  'observation_id',
                  'person_id',
                  'visit_occurrence_id',
                  'observation_concept_id',
                  'observation_datetime',
                  'value_as_concept_id',
                  'value_as_datetime',
                  'value_as_number',
                  'value_as_string',
                  'unit_concept_id',
                  'unit_source_value'
                  )
      if (!is.null(concepts)) {
        concepts <- paste(concepts, collapse=', ')
        where_clause <- paste('WHERE observation_concept_id IN (', concepts, ')')
      }
    }
    if (table_name  == 'measurement') {
      fields <- c(
                  'measurement_id',
                  'person_id',
                  'visit_occurrence_id',
                  'measurement_concept_id',
                  'measurement_datetime',
                  'value_as_number',
                  'unit_concept_id',
                  'unit_source_value'
                  )
      if (!is.null(concepts)) {
        concepts <- paste(concepts, collapse=', ')
        where_clause <- paste('WHERE measurement_concept_id IN (', concepts, ')')
      }
    }
  }

  field_string <- paste(fields, collapse=', ')

  # Prepare variables
  schema_table <- paste(target_schema, table_name, sep='.')
  i <- 1
  dts <- list()

  # count rows for progress bar
  rlang::inform(paste('--- Preparing to extract data from', table_name))
  query_n_rows <- paste('SELECT count(*) FROM ', schema_table, where_clause)
  query_n_rows <- DBI::dbGetQuery(ctn, query_n_rows)[[1]]

  # Prepare query
  query <- paste('SELECT', field_string, 'FROM', schema_table, where_clause)
  sq <- DBI::dbSendQuery(ctn, query)

  # Prepare progress bar
  pb <- progress::progress_bar$new(
    format = "(:spin) [:bar] :percent",
    total = query_n_rows/chunk_size, clear = FALSE, width = 60)
  rlang::inform(paste('--- Starting to extract data from', table_name))

  # Extraction loop
  while (!DBI::dbHasCompleted(sq)) {
    pb$update(i/query_n_rows)
    chunk <- DBI::dbFetch(sq, chunk_size)
    dts[[i]] <- chunk
    i <- i + chunk_size
  }

  DBI::dbClearResult(sq)
  rlang::inform(paste('\n--- Completed data extraction from', table_name))

  # Collapse down into single data.table
  dt <- data.table::rbindlist(dts)

  # Return data.table or data.frame
  if (returnDT) return(dt) else return(setDF(dt))
}


