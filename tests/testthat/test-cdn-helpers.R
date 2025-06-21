test_that("has_object detects object presence/absence", {
  skip_if_offline()
  skip_if_not(nzchar(Sys.getenv("ART_BUCKETS_KEY_ID")))

  tmp <- tempfile(fileext = ".txt")
  writeLines("hello", tmp)
  bucket <- "art-public"
  key <- stringr::str_glue("test-helpers/{fs::path_file(tmp)}")
  s3 <- .get_s3()

  # ensure clean slate
  if (has_object(bucket, key)) {
    s3$delete_object(Bucket = bucket, Key = key)
  }
  expect_false(has_object(bucket, key))

  # upload & re-test
  s3$put_object(
    Body = readBin(tmp, what = "raw", n = file.info(tmp)$size),
    Bucket = bucket, Key = key
  )
  expect_true(has_object(bucket, key))

  # cleanup
  s3$delete_object(Bucket = bucket, Key = key)
})

test_that("has_prefix detects prefix presence/absence", {
  skip_if_offline()
  skip_if_not(nzchar(Sys.getenv("ART_BUCKETS_KEY_ID")))

  tmp <- tempfile(fileext = ".txt")
  writeLines("world", tmp)
  bucket <- "art-public"
  prefix <- "test-helpers-dir"
  key <- stringr::str_glue("{prefix}/{fs::path_file(tmp)}")
  s3 <- .get_s3()

  # ensure clean slate
  if (has_object(bucket, key)) {
    s3$delete_object(Bucket = bucket, Key = key)
  }
  expect_false(has_prefix(bucket, prefix))

  # upload & re-test
  s3$put_object(
    Body = readBin(tmp, what = "raw", n = file.info(tmp)$size),
    Bucket = bucket, Key = key
  )
  expect_true(has_prefix(bucket, prefix))

  # cleanup
  s3$delete_object(Bucket = bucket, Key = key)
})
