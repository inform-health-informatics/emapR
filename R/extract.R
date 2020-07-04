#' @title Extract & Reshape Data from EMAP
#'
#' This is the workhorse function that transcribes data from EMAP OPS from OMOP
#' CDM 5.3.1 to a standard rectangular table with 1 column per dataitem and 1
#' row per time per patient.
#'
#' The time unit is user definable, and set by the "cadence" argument. The
#' default behaviour is to produce a table with 1 row per hour per patient. If
#' there are duplicates/conflicts (e.g more than 1 event for a given hour), then
#' the default behaviour is that only the first result for that hour is
#' returned. One can override this behvaiour by supplying a vector of summary
#' functions directly to the 'coalesce_rows' argument. This could also include
#' any custom function written by the end user, so long as it takes a vector of
#' length n, and returns a vector of length 1, of the original data type.
#'
#' Many events inside EMAP occur on a greater than hourly basis. Depending
#' upon the chosen analysis, you may which to increase the cadence. 0.5 for
#' example will produce a table with 1 row per 30 minutes per patient. Counter
#' to this, 24 would produce 1 row per 24 hours.
#'
#' Choose what variables you want to pull out wisely. This function is quite
#' efficient considering what it needs to do, but it can take a very long
#' time if extracting lots of data, and doing so repeatedly. It is a strong
#' recomendation that you run your extraction on a small subset of patients
#' first and check that you are happy with the result, before moving to a larger
#' extraction.
#'
#' The current implementation is focussed on in-patients only. And as such, all
#' dataitems are referenced to the visit_start_datetime of the visit_occurrence.
#' Thus, observations and measurements recorded outside the boudaries of the
#' visit_occurrence are automatically removed. This is - at this stage -
#' intensional behaviour.
#'
#' @param connection a EMAP database connection
#' @param target_schema the target database schema
#' @param visit_occurrence_ids an integer vector of episode_ids or NULL. If NULL
#'   (the default) then all visits are extracted.
#' @param concept_names a vector of OMOP concept_ids to be extracted
#' @param rename a character vector of names you want to relabel OMOP codes
#'   as, or NULL (the default) if you do not want to relabel. Given in the same
#'   order as \code{concept_names}
#' @param coalesce_rows a vector of summary functions that you want to summarise
#'   data that is contributed higher than your set cadence. Given in the same
#'   order as \code{concept_names}
#' @param chunk_size a chunking parameter to help speed up the function and
#'   manage memory constaints. The defaults work well for most desktop
#'   computers.
#' @param cadence a numerical scalar >= 0. Describes the base time unit to build
#'   each row, in divisions of an hour. For example: 1 = 1 hour, 0.5 = 30 mins,
#'   2 = 2 hourly. If cadence = 0, then the pricise datetime will be used to
#'   generate the time column. This is likely to generate a large table, so use
#'   cautiously.
#'
#' @return sparse tibble with hourly cadence as rows, and unique OMOP concepts
#'   as columns.
#'
#' @export
#'
#' @import data.table
#'
#' @importFrom purrr map imap
#' @importFrom lubridate now
#' @importFrom rlang inform
#' @importFrom dplyr first
#'
#' @export
extract <- function(connection,
                    target_schema,
                    visit_occurrence_ids = NULL,
                    concept_names = NULL,
                    rename = NULL,
                    coalesce_rows = NULL,
                    chunk_size = 5000,
                    cadence = 1) {

  rlang::inform('--- NOTE: Not using chunksize argument at the moment')
  rlang::inform('--- NOTE: `visit_occurrence_ids` are actually visit_detail_ids')
  starting <- now()

  tables <- c('concepts', 'visit_detail', 'observation')
  for (i in 1:length(tables)) {
    t <- DBI::Id(schema=target_schema, table=tables[i])
    chk <- DBI::dbExistsTable( connection,t)
    assertthat::assert_that(chk, msg=paste('!!! unable to find table', tables[i], 'in connection'))
  }
  # Load visit details
  vd <- select_star_from(ctn, target_schema, 'visit_detail')  # ICU department visits; unique key = csn + dt_admit
  # Load and prepare the concepts
  concepts <- select_star_from(ctn, target_schema, 'concepts')

  if (!(is.null(visit_occurrence_ids) | is.vector(visit_occurrence_ids)) ) {
    rlang::abort( "`visit_occurrence_ids` must be given as NULL (the default)")
  }

  # Now define a table of anchor times to make the observation time relative
  # e.g. following hospital admission, following ICU admission
  # The following will align everything to the beginning of the ICU admission
  vd <- vd[,.(visit_detail_id, visit_detail_start_datetime)]

  cadence_pos_num <- class(cadence) == "numeric" && cadence >= 0
  cadence_timestamp <- cadence == "timestamp"

  if (!(cadence_pos_num || cadence_timestamp)) {
    rlang::abort(
      "`cadence` must be given as a numeric scalar >= 0
       or the string 'timestamp'")
  }

  # this will work for non-numeric data too
  # either one function for all (or) one function for each item
  if (is.null(coalesce_rows)) {
    rlang::inform("--- Using first to select values where more than one available")
  }
  coalesce_rows <- parse_coalesce_functions(coalesce_rows)

  # check that we've either got one function to recycle or one function per variable
  chk1 <- is.function(coalesce_rows)
  chk2 <- length(coalesce_rows)
  chk3 <- length(coalesce_rows) == length(concept_short_names)
  assertthat::assert_that(any(chk1 | chk2 | chk3))


  # build the parameter table as per inspectEHR::extract
  params <- data.table(
    short_name = concept_short_names,
    func = coalesce_rows[[3]], # short name; should be OK for functions in the global env
    func_names_full = unlist(coalesce_rows[[2]])
    )
  params <- params[concepts[,.(concept_id, target, short_name)], on='short_name', nomatch=0]
  params <- unique(params)
  params$i_concept_names <- paste0('i', params$concept_id)

  assertthat::assert_that(anyDuplicated(params[,.(short_name,func)]) == 0)

  # if more than one summary function provided for a variable then append that to the column name
  if (anyDuplicated(params$short_name)) {
    params[, col_name := paste(short_name, func, sep='_')]
  } else {
    params[, col_name := short_name]
  }

  rlang::inform('\n--- BEGIN: parameters to be processed\n')
  print(params)
  rlang::inform('\n--- END: parameters to be processed')


  rlang::inform('--- NOTE: Potentially slow query; should take around 1 minute')
  obs <- select_star_from(ctn, 'icu_audit', 'observation')
  rlang::inform(paste('--- NOTE: Loaded', nrow(obs), 'observations'))

  # TODO: add in measurements
  # measurements <- select_star_from(ctn, 'icu_audit', 'measurements')

  # Filter to just the relevant concepts for the relevant patients
  tdt <- filter_obs(obs, params$concept_id, visit_occurrence_ids)
  # Standardise the naming
  tdt <-  rename_obs(tdt)
  tdt <- make_times_relative(tdt,vdt)

  # https://stackoverflow.com/questions/26508519/how-to-add-elements-to-a-list-in-r-loop
  tdts <- vector("list", nrow(params))

  for (i in 1:length(tdts)) {
    suppressWarnings( param <- params[i,] ) # warning from tibble around unknown columns?
    udt <- tdt[concept_id == param$concept_id]
    udt <- coalesce_over(udt, cadence=cadence)
    udt[, col_name := param$col_name]
    print(paste('*** Coalesced', param$short_name, "from", nrow(tdt), "rows to", nrow(udt), "rows at a", cadence, "hourly cadence using", param$func))
    tdts[[i]] <- udt

  }

  res <- rbindlist(tdts)

  elapsed_time <- signif(
    as.numeric(
      difftime(
        lubridate::now(), starting, units = "hour")), 2)
  rlang::inform(paste(elapsed_time, "hours to process"))

  if (requireNamespace("praise", quietly = TRUE)) {
    well_done <-
      praise::praise(
        "${EXCLAMATION}! How ${adjective} was that?!"
      )
    rlang::inform(well_done)
  }

  return(res)
}


# Helper functions
# ================
# Not exported

filter_obs <- function(dt, concept_ids, these_ids=NULL){
  'filter observations by concept_id and episode (aka visit_occurrence)'
  tdt <- data.table::copy(dt)
  tdt <- tdt[observation_concept_id %chin% concept_ids]
  if (!is.null(visit_occurrence_ids)) {
    tdt <- tdt[visit_occurrence_id %in% these_ids]
  }
  return(tdt)
}

rename_obs <- function(dt){
  'rename obs table in a standardised way so can merge with measurements'
  # TODO: convert this function so that it can produce a standardised table
  # regardless of the source input
  # that is you can read directly from OMOP, from your star table etc
  cols <- c('person_id',
            'visit_occurrence_id',
            'visit_detail_id',
            'observation_datetime',
            'observation_concept_id',
            'value_as_concept_id',
            'value_as_datetime',
            'value_as_number',
            'value_as_string'
            )
  tdt <- dt[,..cols, with=TRUE]
  setnames(tdt, 'observation_datetime', 'datetime')
  setnames(tdt, 'observation_concept_id', 'concept_id')
  return(tdt)
}

make_times_relative <- function(dt, vd, units = "hours", debug=FALSE) {
  'given a timeseries keyed by an id, and a start time for each id'
  'vd = visit_detail with visit_detail_start_datetime'
  'dt = obs or similar with visit_occurrence_id and datetime'
  'NOTE: returns time diff in hours'
  # TODO: convert this to work with any pair of tables
  # where one contains timeseries EAV and the other has an 'offset' date against an ID
  tdt <- data.table::copy(dt)
  assertthat::assert_that(uniqueN(vdt) == nrow(vdt))

  tdt <- vdt[tdt, on=c('visit_detail_id')]
  tdt[, diff_time := as.numeric(difftime(datetime, visit_detail_start_datetime, units = units))]
  tdt <- tdt[order(visit_detail_id, diff_time)]
  if (!debug) {
    tdt[, visit_detail_start_datetime := NULL]
    tdt[, datetime := NULL]
  }
  if (NA %in% tdt$diff_time){
    rlang::warn('--- Lossy: NAs generated when calculating diff, missing either visit_detail_start_datetime or similar')
    # Not necessarily a problem since there will be observations for visits that you're not interested in
    # remember that the join is on the specific visit detail time
    tdt <- tdt[!is.na(diff_time)]
  }
  setcolorder(tdt, c('person_id', 'visit_occurrence_id', 'visit_detail_id', 'diff_time', 'concept_id'))
  return(tdt)
}

coalesce_over <- function(dt, value_as='number', coalesce=NULL, cadence=1) {
  'given dt with diff times, collapse using function over cadence'

  value_as <- paste0('value_as_', value_as)
  # TODO: where value_as_string/datetime etc. then build in supporting logic
  cols <- paste(c('visit_detail_id', 'diff_time', value_as))

  if (is.null(coalesce)) coalesce <- "first"

  dt <- dt[,..cols,with=TRUE]
  dt[, diff_time := round_any(diff_time, cadence)]
  dt[, value_as_number := do.call(coalesce, list(get(value_as))), by=.(visit_detail_id, diff_time)]
  return(unique(dt))
}


parse_coalesce_functions <- function(funs=NULL, default_fun=data.table::first){
  #' unpack a function vector and return a list of lists (functions, and names)
  #' necessary because a list or vector of functions does not keep its names
  assertthat::assert_that(is.null(funs) | is.vector(funs) | is.function(funs))

  if (is.null(funs)) {
    res <- list(default_fun, deparse(substitute(default_fun)))
  }


  if (is.function(funs)) {
    res <- list(funs, deparse(substitute(funs)))
  }

  if (is.vector(funs)) {
    assertthat::assert_that(all(sapply(funs, is.function)))
    funs_names <- as.list(as.character(substitute(funs)))
    # vector of functions c(sum, mean) will be parsed to c('c', 'sum', 'mean') so drop first item
    funs_names <- funs_names[2:length(funs_names)]
    res <- list(funs, funs_names)
  }

  res[[1]] <- unlist(res[[1]])
  res[[2]] <- unlist(sapply(res[[2]], eval))
  # unpack where functions are passed as data.table::first etc
  res[[3]] <- unlist(lapply(lapply(strsplit(unlist(res[[2]]), '::| +'), rev), `[[`, 1))

  return(res)

}

#' Fill in 2d Table to make a Sparse Table
#'
#' The extract_timevarying returns a non-sparse table (i.e. rows/hours with
#' no recorded information for a patient are not presented in the table)
#' This function serves to expand the table and fill missing rows with NAs.
#' This is useful when working with most time-series aware stats packages
#' that expect a regular cadence to the table.
#'
#' @param df a dense time series table produced from extract_timevarying
#' @param cadence the cadence by which you want to expand the table
#'   (default = 1 hour)
#'
#' @return a sparse time series table
#' @export
expand_missing <- function(df, cadence = 1) {
  df %>%
    select(episode_id, time) %>%
    split(., .$episode_id) %>%
    imap(function(base_table, epi_id) {
      tibble(
        episode_id = as.numeric(epi_id),
        time = seq(
          min(base_table$time, 0),
          max(base_table$time, 0),
          by = cadence
        )
      )
    }) %>%
    bind_rows() %>%
    left_join(df, by = c("episode_id", "time"))
}
