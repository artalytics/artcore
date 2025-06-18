# tests/testthat/test-cdn-connect.R

library(withr)


test_that("cdn_connect returns default subdomain endpoint and credentials", {
  # Mock environment variable for read-public-assets
  local_envvar(ART_READ_PUBLIC_KEY = "test-public-key")

  cfg <- cdn_connect("public.artalytics", scope = "read")

  expect_type(cfg, "list")
  expect_equal(cfg$bucket, "public.artalytics")
  expect_equal(cfg$key_id, "DO801WYB34KBG47CMXTZ")
  expect_equal(cfg$secret_key, "test-public-key")
  expect_match(cfg$endpoint, "^https://public\\.artalytics\\.art")
})

test_that("cdn_connect returns specified endpoint with credentials", {
  local_envvar(ART_READ_PUBLIC_KEY = "test-public-key")

  cfg_cdn <- cdn_connect("public.artalytics", scope = "read", endpoint = "cdn")

  expect_equal(cfg_cdn$bucket, "public.artalytics")
  expect_equal(cfg_cdn$key_id, "DO801WYB34KBG47CMXTZ")
  expect_equal(cfg_cdn$secret_key, "test-public-key")
  expect_match(cfg_cdn$endpoint, "^https://public\\.artalytics\\.sfo3\\.cdn\\.digitaloceanspaces\\.com")
})

test_that("cdn_connect errors for unsupported scope", {
  expect_error(
    cdn_connect("public.artalytics", scope = "write"),
    "No 'write' key configured for bucket 'public.artalytics'"
  )
})


test_that("cdn_connect errors for unknown bucket", {
  expect_error(
    cdn_connect("nonexistent.bucket", scope = "read"),
    "Bucket 'nonexistent.bucket' not found in do-spaces\\.yaml\\."
  )
})

test_that("cdn_connect(client = TRUE) returns a client list with S3 methods", {
  skip_if_not_installed("paws.storage")
  local_envvar(ART_READ_PRIVATE_KEY = "dummy-private-key")

  client <- cdn_connect("private.artalytics", scope = "read", client = TRUE)

  # It should at least be a list
  expect_type(client, "list")

  # And it must implement the core S3 operations
  expect_true(is.function(client$list_buckets),
    info = "client must have a list_buckets() method"
  )
  expect_true(is.function(client$get_object),
    info = "client must have a get_object() method"
  )
  expect_true(is.function(client$put_object),
    info = "client must have a put_object() method"
  )
})
