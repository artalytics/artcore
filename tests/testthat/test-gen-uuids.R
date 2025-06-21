test_that("Generate UUIDs", {
  genCollectionID() |>
    stringr::str_sub(1, 2) |>
    (function(x) x == "77")() |>
    expect_true()

  genArtistID() |>
    stringr::str_sub(1, 2) |>
    (function(x) x == "88")() |>
    expect_true()

  genArtworkID() |>
    stringr::str_sub(1, 2) |>
    (function(x) x == "99")() |>
    expect_true()

  genCollectionID(test = TRUE) |>
    stringr::str_sub(1, 2) |>
    (function(x) x == "70")() |>
    expect_true()

  genArtistID(test = TRUE) |>
    stringr::str_sub(1, 2) |>
    (function(x) x == "80")() |>
    expect_true()

  genArtworkID(test = TRUE) |>
    stringr::str_sub(1, 2) |>
    (function(x) x == "90")() |>
    expect_true()
})
