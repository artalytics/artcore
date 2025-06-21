# # tests/testthat/test-cdn-write.R

artist <- genArtistID(test = TRUE)
artwork <- genArtworkID(test = TRUE)



# art-coa -----------------------------------------------------------------


test_that("write_art_coa uploads one cert", {
  skip_if_offline()
  skip_if_not(nzchar(Sys.getenv("ART_BUCKETS_KEY_ID")))
  tmp <- tempfile(fileext = ".pdf")
  writeLines("demo", tmp)

  key <- stringr::str_glue("issued/{artist}/{artwork}/{fs::path_file(tmp)}")

  # ensure clean slate
  if (has_object("art-coa", key)) {
    .get_s3()$delete_object(Bucket = "art-coa", Key = key)
  }

  out <- write_art_coa(artist, artwork, tmp)
  expect_equal(out, key)
  expect_true(has_object("art-coa", key))
  expect_error(write_art_coa(artist, artwork, tmp))

  # cleanup
  .get_s3()$delete_object(Bucket = "art-coa", Key = key)
})



# art-public --------------------------------------------------------------



test_that("write_art_public uploads artist and artwork thumbs", {
  skip_if_offline()
  skip_if_not(nzchar(Sys.getenv("ART_BUCKETS_KEY_ID")))

  # ---------- artist thumbnail ----------
  f1 <- "test-files/image.jpeg"
  k1 <- stringr::str_glue("thumbnails/artist/{artist}.jpeg")

  if (has_object("art-public", k1)) {
    .get_s3()$delete_object(Bucket = "art-public", Key = k1)
  }

  write_art_public(artist = artist, local_path = f1) |>
    expect_equal(k1)

  has_object("art-public", k1) |>
    expect_true()

  .get_s3()$delete_object(Bucket = "art-public", Key = k1)

  # ---------- artwork thumbnail ----------
  k2 <- stringr::str_glue("thumbnails/{artist}/{artwork}.jpeg")
  if (has_object("art-public", k2)) {
    .get_s3()$delete_object(Bucket = "art-public", Key = k2)
  }

  expect_equal(write_art_public(artist, artwork, f1), k2)
  expect_true(has_object("art-public", k2))
  .get_s3()$delete_object(Bucket = "art-public", Key = k2)
})


# art-data ----------------------------------------------------------------

test_that("write_art_data uploads a dir and single file", {
  skip_if_offline()
  skip_if_not(nzchar(Sys.getenv("ART_BUCKETS_KEY_ID")))

  # ---------- directory ----------
  dir_local <- fs::dir_create(tmpdir <- tempfile("art_"))
  writeLines("x", fs::path(tmpdir, "a.txt"))

  fs::dir_create(file.path(tmpdir, "sub"))
  writeLines("y", fs::path(tmpdir, "sub", "b.txt"))

  keys <- write_art_data(artist, artwork, tmpdir, delete_local = TRUE)
  expect_true(all(.keys_exist("art-data", keys)))

  # cleanup
  s3 <- .get_s3()


  delete <- lapply(keys, function(k) setNames(list(k), "Key")) |>
    list() |>
    setNames("Objects")
  s3$delete_objects(Bucket = "art-data", Delete = delete)


  # ---------- single file ----------
  f <- tempfile(fileext = ".txt")
  writeLines("z", f)
  single_key <- stringr::str_glue(
    "processed/{artist}/{artwork}/{fs::path_file(f)}"
  )

  ## Scenario a: no prefix provided for single file upload
  ##
  res <- write_art_data(artist, artwork, local_path = f, delete_local = TRUE)
  expect_equal(as.character(res), single_key)


  expect_true(has_object("art-data", single_key))

  s3$delete_object(Bucket = "art-data", Key = single_key)
})




# art-vault ---------------------------------------------------------------

test_that("write_art_vault uploads dir and blocks overwrite", {
  skip_if_offline()
  skip_if_not(nzchar(Sys.getenv("ART_BUCKETS_KEY_ID")))

  # ---------- prepare dir -----------
  vault_dir <- fs::dir_create(tmp <- tempfile("vault_"))
  writeLines("canvas", file.path(tmp, "canvas.procreate"))
  writeLines("readme", file.path(tmp, "readme.txt"))

  prefix <- stringr::str_glue("uploads/{artist}/{artwork}")
  keys <- fs::path(prefix, fs::path_rel(
    fs::dir_ls(tmp, recurse = TRUE, type = "file"), tmp
  ))

  # ensure clean slate
  s3 <- .get_s3()
  if (has_prefix("art-vault", prefix)) {
    for (k in keys) s3$delete_object(Bucket = "art-vault", Key = k)
  }

  # first upload should succeed
  out <- write_art_vault(artist, artwork, tmp)
  expect_setequal(out, keys)
  expect_true(all(.keys_exist("art-vault", keys)))

  # second upload should error (prefix exists)
  expect_error(
    write_art_vault(artist, artwork, tmp),
    "Vault prefix already exists"
  )

  # cleanup
  for (k in keys) {
    s3$delete_object(Bucket = "art-vault", Key = k)
  }
})
