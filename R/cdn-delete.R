#' Soft-delete an artwork across all Spaces buckets
#'
#' Copies then removes every object for the given `artist` / `artwork` in:
#' - art-vault (`uploads/artist/artwork/...` => `uploads/artist/_deleted-artwork/...`)
#' - art-data  (`processed/artist/artwork/...`=>`processed/artist/_deleted-artwork/...`)
#' - art-coa   (`issued/artist/artwork/...`=>`issued/artist/_deleted-artwork/...`)
#' - art-public (single key move:
#'     `thumbnails/artist/artwork.jpeg`=>`thumbnails/artist/_deleted-artwork.jpeg`)
#'
#' @param artist   Character(1). UUID of the artist.
#' @param artwork  Character(1). UUID of the artwork.
#' @param dry_run  Logical. If `TRUE`, only logs planned actions; does _not_ perform copy or delete.
#'                 Defaults to `FALSE`.
#'
#' @return Invisibly, a named list of character vectors giving the new keys for each bucket.
#' @examples
#' \dontrun{
#' cdn_soft_delete(
#'   artist   = "80000000-0000-0000-0000-000000000000",
#'   artwork  = "90000000-0000-0000-0000-000000000000",
#'   dry_run  = TRUE
#' )
#' }
#'
#' @importFrom stringr str_glue
#' @importFrom fs path_dir path_file
#' @importFrom paws.storage s3
#' @importFrom rdstools log_inf log_suc
#' @export
cdn_soft_delete <- function(artist, artwork, dry_run = FALSE) {
  stopifnot("Invalid Artist UUID" = is.character(artist) & length(artist) == 1L)
  stopifnot("Invalid Artwork UUID" = is.character(artwork) & length(artwork) == 1L)

  ## Check to make sure objects exist at expected prefixes or key locations
  # verify the artwork actually exists before moving
  vault_pref <- stringr::str_glue("uploads/{artist}/{artwork}")
  data_pref <- stringr::str_glue("processed/{artist}/{artwork}")
  coa_pref <- stringr::str_glue("issued/{artist}/{artwork}")
  thumb_key <- stringr::str_glue("thumbnails/{artist}/{artwork}.jpeg")

  if (!has_prefix("art-vault", vault_pref)) {
    stop("No objects found in art-vault at: ", vault_pref, call. = FALSE)
  }
  if (!has_prefix("art-data", data_pref)) {
    stop("No objects found in art-data at: ", data_pref, call. = FALSE)
  }
  if (!has_prefix("art-coa", coa_pref)) {
    stop("No objects found in art-coa at: ", coa_pref, call. = FALSE)
  }
  if (!has_object("art-public", thumb_key)) {
    stop("No public thumbnail found at: ", thumb_key, call. = FALSE)
  }

  s3 <- .get_s3()

  # mapping of bucket=>old-prefix template
  buckets <- list(
    "art-vault" = vault_pref,
    "art-data"  = data_pref,
    "art-coa"   = coa_pref
  )

  results <- list()

  for (bkt in names(buckets)) {
    # build prefixes
    old_pref <- stringr::str_glue(buckets[[bkt]])
    new_pref <- stringr::str_glue(
      "{fs::path_dir(old_pref)}/_deleted-{fs::path_file(old_pref)}"
    )

    log_msg <- stringr::str_glue(
      "=> [{bkt}] prefix '{old_pref}/'=>'{new_pref}/'"
    )
    if (dry_run) {
      rdstools::log_inf("DRY RUN", log_msg)
      next
    }
    rdstools::log_inf(bkt, log_msg)


    # 1) list ALL objects under old prefix (pagination-safe)
    old_keys <- .list_all_keys(s3, bkt, old_pref)

    if (length(old_keys) == 0L) {
      rdstools::log_inf(bkt, paste0("No objects found under '", old_pref, "/'"))
      results[[bkt]] <- character(0)
      next
    }

    new_keys <- sub(
      paste0("^", old_pref),
      new_pref,
      old_keys,
      perl = TRUE
    )

    # 2) copy each object server-side
    mapply(function(src, dst) {
      rdstools::log_inf(bkt, paste0("Copy: ", src, "=>", dst))
      s3$copy_object(
        Bucket     = bkt,
        CopySource = paste0(bkt, "/", src),
        Key        = dst
      )
    }, old_keys, new_keys, USE.NAMES = FALSE)

    # 3) batch-delete originals
    delete_list <- list(
      Objects = lapply(old_keys, function(k) list(Key = k))
    )
    resp <- s3$delete_objects(Bucket = bkt, Delete = delete_list)
    if (!is.null(resp$Errors) && length(resp$Errors) > 0) {
      stop(sprintf(
        "[%s] delete_objects reported errors: %s",
        bkt,
        paste0(vapply(resp$Errors, `[[`, "", "Key"), collapse = ", ")
      ), call. = FALSE)
    }

    rdstools::log_suc(bkt, paste0(
      "Moved ", length(old_keys),
      " objects to '", new_pref, "/'"
    ))
    results[[bkt]] <- new_keys
  }

  # art-public: single artwork thumbnail only
  thumb_old <- thumb_key
  thumb_new <- stringr::str_glue("thumbnails/{artist}/_deleted-{artwork}.jpeg")

  log_msg <- paste0("=> [art-public] key '", thumb_old, "' => '", thumb_new, "'")

  if (dry_run) {
    rdstools::log_inf("DRY RUN", log_msg)
  } else {
    rdstools::log_inf("art-public", paste("Copy:", thumb_old, "=>", thumb_new))
    s3$copy_object(
      Bucket     = "art-public",
      CopySource = paste0("art-public/", thumb_old),
      Key        = thumb_new
    )
    s3$delete_objects(
      Bucket = "art-public",
      Delete = list(Objects = list(list(Key = thumb_old)))
    )
    rdstools::log_suc("art-public", paste("Moved thumbnail to", thumb_new))
  }
  results[["art-public"]] <- thumb_new

  invisible(results)
}




# artist <- "80000000-0000-0000-0000-000000000000"
# artwork <- "90000000-0000-0000-0000-000000000000"
#
# s3 <- .get_s3()
#
# bucket <- "art-data"
# prefix <- stringr::str_glue("processed/{artist}/{artwork}")
#
# dropKeys <- s3$list_objects(
#   Bucket = bucket,
#   Prefix = prefix,
# )$Content |>
#   lapply(function(i) list(Key = i$Key)) |>
#   list() |>
#   setNames("Objects")
#
# s3$delete_objects(Bucket = bucket, Delete = dropKeys)
