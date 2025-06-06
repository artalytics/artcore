#' Package utils
#'
#' @param cn connection object
#'
#' @importFrom rpgconn dbc dbd
#' @importFrom rdstools log_suc
#'
#' @name utils
NULL


#' @describeIn utils tbd
#' @export
..dir_ad <- function() {
  ad <- Sys.getenv("ARTDATA")
  stopifnot("ARTDATA Not Set" = ad != "")
  ad
}


#' @describeIn utils tbd
#' @export
..dbc <- function() {
  cfg <- Sys.getenv("ARTCONF")
  stopifnot("ARTCONF Not Set" = cfg != "")
  cn <- suppressMessages(rpgconn::dbc(cfg, "artalytics"))
  rdstools::log_suc(paste0("Connected to DB (", cfg, ")"))
  cn
}


#' @describeIn utils tbd
#' @export
..dbd <- function(cn) {
  cfg <- Sys.getenv("ARTCONF")
  suppressMessages(rpgconn::dbd(cn))
  rdstools::log_inf(paste0("Disconnected from DB (", cfg, ")"))
  invisible(NULL)
}


#' @describeIn utils tbd
#' @export
..is_demo <- function() {
  isTRUE(as.logical(Sys.getenv("ARTDEMO", "FALSE")))
}
