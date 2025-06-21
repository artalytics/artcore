#' Gen UUIDs
#'
#' @param test bool
#'
#' @name gen-uuid
NULL


#' @describeIn gen-uuid internal
#' @export
genArtistID <- function(test = FALSE) {
  uuid <- uuid::UUIDgenerate()
  if (test) {
    stringr::str_sub(uuid, 1, 8) <- "80000000"
  } else {
    stringr::str_sub(uuid, 1, 2) <- "88"
  }
  uuid
}


#' @describeIn gen-uuid internal
#' @export
genArtworkID <- function(test = FALSE) {
  uuid <- uuid::UUIDgenerate()
  if (test) {
    stringr::str_sub(uuid, 1, 8) <- "90000000"
  } else {
    stringr::str_sub(uuid, 1, 2) <- "99"
  }
  uuid
}


#' @describeIn gen-uuid internal
#' @export
genCollectionID <- function(test = FALSE) {
  uuid <- uuid::UUIDgenerate()
  if (test) {
    stringr::str_sub(uuid, 1, 8) <- "70000000"
  } else {
    stringr::str_sub(uuid, 1, 2) <- "77"
  }
  uuid
}
