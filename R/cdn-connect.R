#' Connect to DigitalOcean Spaces / CDN
#'
#' Retrieve bucket credentials and endpoint URLs for your configured DigitalOcean
#' Spaces buckets (via CDN subdomains, direct CDN endpoints, or origin URLs).
#' Optionally, return a ready-to-use S3 client for programmatic access.
#'
#' @param bucket    Character. One of the bucket names defined in
#'                  `inst/config/do-spaces.yaml`:
#'                  `"public.artalytics"`, `"private.artalytics"`,
#'                  `"certificates.art"`, or `"vault.artalytics"`.
#' @param scope     Character. Access scope to retrieve; must match one of the
#'                  lowercase scopes in your YAML (`"read"` or `"write"`).
#'                  Defaults to `"read"`.
#' @param endpoint  Character. Which endpoint URL to return. One of
#'                  `"subdomain"`, `"cdn"`, or `"origin"`.
#'                  Defaults to `"subdomain"`.
#' @param client    Logical. If `TRUE`, returns a configured
#'                  `paws.storage::s3()` client object instead of a list.
#'                  Defaults to `FALSE`.
#'
#' @return
#' If `client = FALSE`, a named list with components:
#' * `bucket`     — the bucket name (character)
#' * `key_id`     — the Spaces access key ID (character)
#' * `secret_key` — the Spaces secret access key (character)
#' * `endpoint`   — the selected endpoint URL (character)
#'
#' If `client = TRUE`, returns an S3 client object (list) with methods
#' such as `$list_buckets()`, `$get_object()`, and `$put_object()`.
#'
#' @examples
#' \dontrun{
#' # 1. Get the public subdomain URL + credentials
#' cfg <- cdn_connect("public.artalytics", scope = "read")
#' print(cfg$endpoint)
#'
#' # 2. Get the private CDN endpoint
#' cdn_cfg <- cdn_connect("private.artalytics", scope = "read", endpoint = "cdn")
#'
#' # 3. Spin up an S3 client for writing
#' s3 <- cdn_connect("private.artalytics", scope = "write", client = TRUE)
#' s3$list_buckets()
#' }
#'
#' @importFrom yaml read_yaml
#' @importFrom utils packageName
#' @importFrom fs path_package
#' @importFrom paws.storage s3
#'
#'
#' @export
cdn_connect <- function(
    bucket = c(
      "public.artalytics",
      "private.artalytics",
      "certificates.art",
      "vault.artalytics"
    ),
    scope = c("read", "write"),
    endpoint = c("subdomain", "cdn", "origin"),
    client = FALSE) {
  # ----- 1. match scope & endpoint normally -----
  scope <- match.arg(scope)
  endpoint <- match.arg(endpoint)

  # ----- 2. bucket: catch match.arg error, defer validation -----
  bucket <- tryCatch(
    match.arg(bucket),
    error = function(e) bucket
  )

  # ----- 3. load & parse YAML -----
  cfg <- .cdn_cfg()

  # ----- 4. validate bucket -----
  if (!bucket %in% names(cfg$buckets)) {
    stop(sprintf("Bucket '%s' not found in do-spaces.yaml.", bucket),
      call. = FALSE
    )
  }
  b_cfg <- cfg$buckets[[bucket]]

  # ----- 5. find matching access block -----
  acc_list <- Filter(function(x) identical(x$scope, scope), b_cfg$access)
  if (length(acc_list) == 0L) {
    stop(sprintf("No '%s' key configured for bucket '%s'.", scope, bucket),
      call. = FALSE
    )
  }
  acc <- acc_list[[1]]

  # ----- 6. return S3 client if requested -----
  if (isTRUE(client)) {
    if (!requireNamespace("paws.storage", quietly = TRUE)) {
      stop("Please install the 'paws.storage' package to use `client = TRUE`.",
        call. = FALSE
      )
    }
    return(
      paws.storage::s3(
        config = list(
          credentials = list(
            creds = list(
              access_key_id     = acc$key_id,
              secret_access_key = acc$key
            )
          ),
          endpoint = b_cfg$endpoints$origin,
          region = "us-east-1" # required by paws, ignored by DO
        )
      )
    )
  }

  # ----- 7. return credentials + selected endpoint -----
  list(
    bucket     = bucket,
    key_id     = acc$key_id,
    secret_key = acc$key,
    endpoint   = b_cfg$endpoints[[endpoint]]
  )
}

# -------------------------------------------------------------------------
# Private helper: memoised YAML loader
.cdn_cfg <- local({
  cfg <- NULL
  function() {
    if (is.null(cfg)) {
      yaml_path <- fs::path_package("config/do-spaces.yaml", package = utils::packageName())
      if (!file.exists(yaml_path)) {
        stop("Cannot find 'config/do-spaces.yaml' in the package.", call. = FALSE)
      }
      cfg <<- yaml::read_yaml(yaml_path, eval.expr = TRUE)
    }
    cfg
  }
})
