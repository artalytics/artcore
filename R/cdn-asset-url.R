#' Construct or Presign a DigitalOcean Spaces Asset URL
#'
#' Builds a public HTTPS URL for any object in your Spaces buckets,
#' optionally presigning it (for private buckets) or checking that it exists.
#'
#' @param bucket  Character(1). One of your Spaces buckets:
#'                     `"art-public"`, `"art-coa"`, `"art-data"`, `"art-vault"`.
#' @param key    Character(1). The object key/path inside the bucket,
#'                     e.g. `"thumbnails/.../img.png"`.  May be `""` to hit the
#'                     bucket root.
#' @param signed       Logical(1).  `TRUE` to generate a presigned URL (needs
#'                     valid `ART_BUCKETS_KEY_ID`/`_KEY_SECRET`), or `FALSE`
#'                     to build a plain URL.  Defaults to `TRUE`.
#' @param check        Logical(1).  If `TRUE`, performs an HTTP HEAD on the
#'                     resulting URL and errors if the status is >= 400.
#'                     Defaults to `FALSE`.
#'
#' @return Character(1). A valid HTTPS URL.  If `check = TRUE` this will
#'         have been HEAD-verified to return HTTP 200.
#'
#' @details
#' - Buckets `"art-public"` and `"art-coa"` are public-read, so any URL
#'   (signed or not) is returned unsigned.
#' - Buckets `"art-data"` and `"art-vault"` require a valid
#'   `ART_BUCKETS_KEY_ID` + `ART_BUCKETS_KEY_SECRET` to presign.
#'
#' @examples
#' \dontrun{
#' # public asset (no creds needed)
#' url1 <- cdn_asset_url("art-public", "thumbnails/foo.png",
#'   signed = FALSE, check = TRUE
#' )
#'
#' # private asset (presigned, 1 h expiry)
#' url2 <- cdn_asset_url("art-data",
#'   "processed/artist/artwork/img.png",
#'   signed = TRUE, check = TRUE
#' )
#' browseURL(url2)
#' }
#'
#' @importFrom paws.storage s3
#' @importFrom httr HEAD status_code
#' @export
cdn_asset_url <- function(bucket = c("art-public", "art-coa", "art-data", "art-vault"),
                          key = "",
                          signed = TRUE,
                          check = FALSE) {
  bucket <- match.arg(bucket)
  key <- sub("^/+", "", key, perl = TRUE)
  root <- "https://sfo3.digitaloceanspaces.com"

  # Public-read buckets: always unsigned
  if (!signed || bucket %in% c("art-public", "art-coa")) {
    url <- paste(root, bucket, key, sep = "/")
  } else {
    # Private buckets: must presign
    ak <- Sys.getenv("ART_BUCKETS_KEY_ID", "")
    sk <- Sys.getenv("ART_BUCKETS_KEY_SECRET", "")
    if (!nzchar(ak) || !nzchar(sk)) {
      stop("ART_BUCKETS_KEY_ID and/or ART_BUCKETS_KEY_SECRET are missing.",
        call. = FALSE
      )
    }

    s3cli <- paws.storage::s3(
      config = list(
        credentials = list(creds = list(
          access_key_id     = ak,
          secret_access_key = sk
        )),
        endpoint = root,
        region = "sfo3",
        s3_force_path_style = FALSE
      )
    )

    url <- s3cli$generate_presigned_url(
      client_method = "get_object",
      params        = list(Bucket = bucket, Key = key),
      expires_in    = 3600
    )
  }

  if (check) {
    res <- httr::GET(url)
    code <- httr::status_code(res)
    if (code >= 400L) {
      stop(sprintf("Asset URL returned HTTP %s", code),
        call. = FALSE
      )
    }
  }

  url
}
