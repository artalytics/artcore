#
# Run test
#
artist <- "80000000-0000-0000-0000-000000000000"
artwork <- "90000000-0000-0000-0000-000000000000"


.restore_art <- function(artist, artwork) {
  ## helper functions
  pref_live <- function(bucket) {
    switch(bucket,
      "art-vault" = stringr::str_glue("uploads/{artist}/{artwork}"),
      "art-data"  = stringr::str_glue("processed/{artist}/{artwork}"),
      "art-coa"   = stringr::str_glue("issued/{artist}/{artwork}")
    )
  }
  pref_deleted <- function(bucket) {
    stringr::str_replace(pref_live(bucket), artwork, paste0("_deleted-", artwork))
  }

  s3 <- .get_s3()

  ## Handle three buckets the same and art-public differently
  buckets <- c("art-vault", "art-data", "art-coa")
  for (bkt in buckets) {
    src_pref <- pref_deleted(bkt)
    dst_pref <- pref_live(bkt)

    objs <- s3$list_objects_v2(
      Bucket = bkt,
      Prefix = paste0(src_pref, "/")
    )$Contents

    if (length(objs) > 0L) {
      src_keys <- vapply(objs, `[[`, "", "Key")
      dst_keys <- sub(src_pref, dst_pref, src_keys, fixed = TRUE)

      mapply(function(src, dst) {
        s3$copy_object(
          Bucket     = bkt,
          CopySource = paste0(bkt, "/", src),
          Key        = dst
        )
      }, src_keys, dst_keys, USE.NAMES = FALSE)

      s3$delete_objects(
        Bucket = bkt,
        Delete = list(Objects = lapply(src_keys, function(k) list(Key = k)))
      )
    }
  }

  thumb_live <- stringr::str_glue("thumbnails/{artist}/{artwork}.jpeg")
  thumb_deleted <- stringr::str_glue("thumbnails/{artist}/_deleted-{artwork}.jpeg")

  # restore thumbnail
  if (has_object("art-public", thumb_deleted)) {
    s3$copy_object(
      Bucket     = "art-public",
      CopySource = paste0("art-public/", thumb_deleted),
      Key        = thumb_live
    )
    s3$delete_objects(
      Bucket = "art-public",
      Delete = list(Objects = list(list(Key = thumb_deleted)))
    )
  }
  return(TRUE)
}


test_that("cdn_soft_delete moves objects and thumbnail, then restores", {
  skip_if_offline()
  skip_if_not(nzchar(Sys.getenv("ART_BUCKETS_KEY_ID")))

  # -------------------------------------------------------------------------
  # on.exit: restore everything back so next test run starts clean
  # -------------------------------------------------------------------------
  on.exit(.restore_art(artist, artwork))

  ## Set test UUIDs
  artist <- "80000000-0000-0000-0000-000000000000"
  artwork <- "90000000-0000-0000-0000-000000000000"

  ## Test helper functions scoped to this test env
  pref_live <- function(bucket) {
    switch(bucket,
      "art-vault" = stringr::str_glue("uploads/{artist}/{artwork}"),
      "art-data"  = stringr::str_glue("processed/{artist}/{artwork}"),
      "art-coa"   = stringr::str_glue("issued/{artist}/{artwork}")
    )
  }
  pref_deleted <- function(bucket) {
    stringr::str_replace(pref_live(bucket), artwork, paste0("_deleted-", artwork))
  }


  thumb_live <- stringr::str_glue("thumbnails/{artist}/{artwork}.jpeg")
  thumb_deleted <- stringr::str_glue("thumbnails/{artist}/_deleted-{artwork}.jpeg")

  # ---- ensure starting state is live (prefix exists) -----------------------
  has_prefix("art-data", pref_live("art-data")) |>
    expect_true()

  # ---- run soft delete -----------------------------------------------------
  expect_no_error(cdn_soft_delete(artist, artwork, dry_run = FALSE))
  expect_error(cdn_soft_delete(artist, artwork, dry_run = FALSE))

  # live prefixes gone, deleted prefixes present
  for (bkt in c("art-vault", "art-data", "art-coa")) {
    expect_false(has_prefix(bkt, pref_live(bkt)))
    expect_true(has_prefix(bkt, pref_deleted(bkt)))
  }
  expect_false(has_object("art-public", thumb_live))
  expect_true(has_object("art-public", thumb_deleted))
})
