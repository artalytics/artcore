test_that("public buckets work unsigned and signed=TRUE ignored", {
  skip_if_offline()
  # unsigned root should succeed
  url1 <- expect_silent(cdn_asset_url("art-public", "", signed = FALSE, check = TRUE))
  expect_true(grepl("^https://sfo3\\.digitaloceanspaces\\.com/art-public", url1))

  # signed=TRUE also just builds plain URL
  url2 <- expect_silent(cdn_asset_url("art-public", "", signed = TRUE, check = TRUE))
  expect_identical(url1, url2)
})

test_that("certificate bucket behaves like public", {
  skip_if_offline()
  expect_silent(cdn_asset_url("art-coa", "", signed = FALSE, check = TRUE))
  expect_silent(cdn_asset_url("art-coa", "", signed = TRUE, check = TRUE))
})

test_that("private buckets require presigning", {
  skip_if_offline()
  cdn_asset_url("art-data", "", signed = TRUE, check = TRUE) |> expect_no_error()
  cdn_asset_url("art-vault", "", signed = TRUE, check = TRUE) |> expect_no_error()
})

test_that("private buckets unsigned fails check", {
  skip_if_offline()
  expect_error(cdn_asset_url("art-data", "", signed = FALSE, check = TRUE))
  expect_error(cdn_asset_url("art-vault", "", signed = FALSE, check = TRUE))
})

test_that("unknown bucket errors", {
  expect_error(cdn_asset_url("no-such-bucket"), regexp = "'arg' should be one of")
})


test_that("with asset key", {
  artist <- "746b8207-72f5-4ab6-8d19-a91d03daec3d"
  artwork <- "99925456-2319-4e71-a337-28ee7e9399f7"
  key <- paste0("processed/", artist, "/", artwork, "/mod-gallery/variant-1.png")
  cdn_asset_url("art-data", key, signed = FALSE, check = TRUE) |> expect_error("Asset URL returned HTTP 403")
  cdn_asset_url("art-data", key, signed = TRUE, check = TRUE) |> expect_no_error()
})
