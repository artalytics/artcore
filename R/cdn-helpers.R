#' DigitalOcean Spaces - Existence Helpers
#'
#' `has_prefix()` and `has_object()` let you test whether objects (keys)
#' or prefixes exist in a DigitalOcean Spaces bucket.  All other functions
#' in this file are internal implementation details.
#'
#' @name cdn-helpers
#'
#' @importFrom paws.storage s3
#' @importFrom rdstools log_inf
#' @importFrom stringr str_remove
#' @importFrom paws paginate paginate_lapply
NULL


#' Check for any objects under a prefix
#'
#' Returns `TRUE` if at least one object exists in `bucket` with the given
#' `prefix` (i.e. any key starts with `prefix/`).
#'
#' @param bucket  Character(1). Name of the Spaces bucket.
#' @param prefix  Character(1). The prefix to check (without trailing slash).
#' @return Logical scalar. `TRUE` if one or more objects are found.
#' @examples
#' \dontrun{
#' has_prefix("art-data", "processed/artistUUID/artUUID")
#' }
#' @export
has_prefix <- function(bucket, prefix) {
  .get_s3() |>
    .prefix_exists(bucket, prefix)
}

#' Check for a single object key
#'
#' Returns `TRUE` if the specified `key` exists in `bucket` (HEAD returns 200).
#'
#' @param bucket  Character(1). Name of the Spaces bucket.
#' @param key     Character(1). The object key (full path) to check.
#' @return Logical scalar. `TRUE` if the object exists, `FALSE` otherwise.
#' @examples
#' \dontrun{
#' has_object("art-public", "thumbnails/artist/UUID.png")
#' }
#' @export
has_object <- function(bucket, key) {
  .get_s3() |>
    .object_exists(bucket, key)
}

# Internal ----------------------------------------------------------------


# internal: instantiate a paws.storage client using admin key

#' @keywords internal
#' @noRd
.get_s3 <- function() {
  ak <- Sys.getenv("ART_BUCKETS_KEY_ID", "")
  sk <- Sys.getenv("ART_BUCKETS_KEY_SECRET", "")
  if (!nzchar(ak) || !nzchar(sk)) {
    stop("ART_BUCKETS_KEY_ID and/or ART_BUCKETS_KEY_SECRET are not set.", call. = FALSE)
  }
  paws.storage::s3(
    config = list(
      credentials = list(creds = list(
        access_key_id     = ak,
        secret_access_key = sk
      )),
      endpoint = "https://sfo3.digitaloceanspaces.com",
      region = "sfo3",
      s3_force_path_style = FALSE
    )
  )
}



# internal: check if object exists

#' @keywords internal
#' @noRd
.object_exists <- function(s3, bucket, key) {
  b <- tryCatch(
    {
      s3$head_object(Bucket = bucket, Key = key)
      rdstools::log_inf(paste0("KEY FOUND (", bucket, ")"), key)
      TRUE
    },
    error = function(c) {
      rdstools::log_inf(paste0("KEY NOT FOUND (", bucket, ")"), key)
      FALSE
    }
  )
  return(b)
}



# internal: check if any key exists under prefix

#' @keywords internal
#' @noRd
.prefix_exists <- function(s3, bucket, prefix) {
  page <- paws::paginate(
    s3$list_objects_v2(
      Bucket  = bucket,
      Prefix  = paste0(stringr::str_remove(prefix, "/$"), "/"),
      MaxKeys = 1L # ask for just one key
    ),
    MaxItems = 1L # paginator stops after first page
  )

  b <- length(page) > 0 && page[[1]]$KeyCount > 0L

  if (b) {
    rdstools::log_inf(paste0("PREFIX FOUND (", bucket, ")"), prefix)
  } else {
    rdstools::log_inf(paste0("PREFIX NOT FOUND (", bucket, ")"), prefix)
  }
  return(b)
}


#' @keywords internal
#' @noRd
.list_all_keys <- function(s3, bucket, prefix) {
  paws::paginate_lapply(
    s3$list_objects_v2(
      Bucket  = bucket,
      Prefix  = paste0(prefix, "/")
    ),
    \(p) vapply(p$Contents, `[[`, "", "Key")
  ) |> unlist(use.names = FALSE)
}


# .prefix_exists <- function(s3, bucket, prefix) {
#   prefix <- paste0(stringr::str_remove(prefix, "/$"), "/")
#
#   res <- s3$list_objects_v2(
#     Bucket = bucket,
#     Prefix = prefix,
#     MaxKeys = 1L
#   )
#   b <- isTRUE(res$KeyCount > 0L)
#
#   if (b) {
#     rdstools::log_inf(paste0("PREFIX FOUND (", bucket, ")"), prefix)
#   } else {
#     rdstools::log_inf(paste0("PREFIX NOT FOUND (", bucket, ")"), prefix)
#   }
#   return(b)
# }
