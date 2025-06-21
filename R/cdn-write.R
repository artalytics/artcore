#' Write Assets to DigitalOcean Spaces Buckets
#'
#' Four helpers to upload files or directories to your Spaces buckets,
#' never overwriting existing assets. Uses a single admin key
#' (`ART_BUCKETS_KEY_ID` / `ART_BUCKETS_KEY_SECRET`) via **paws.storage**.
#'
#' @details
#' * `write_art_coa()` writes a single certificate file (pdf or jpeg) to `art-coa` in `issued/artist/artwork/`.
#' * `write_art_public()` writes artist or artwork thumbnails to `art-public` at `thumbnails/artist/` (art image) or `thumbnails/artist/` (artist image)
#' * `write_art_data()` uploads a directory (or single file) into `art-data` at `processed/artist/artwork/`
#' * `write_art_vault()` uploads a directory (or single file) into `art-vault` in `uploads/artist/artwork/`
#'
#' All helpers error if the target key (or any key under a prefix) already exists.
#'
#' @importFrom stringr str_glue str_remove
#' @importFrom fs file_exists is_file is_dir file_delete dir_delete path_ext path path_rel path_file dir_ls
#' @importFrom rdstools log_inf log_suc log_err
#'
#' @param artist      Character(1). UUID of the artist.
#' @param artwork     Character(1). UUID of the artwork (required where noted).
#' @param local_path  Character(1). Path to a local file or directory.
#' @param prefix      Character(1). S3 prefix (for art-data / art-vault)
#' @param delete_local Logical.  Delete the local file after a verified upload. Defaults to `FALSE`.
#'
#' @return Invisibly, a character vector of S3 key(s) uploaded.
#'
#' @name cdn-write
NULL


#' @describeIn cdn-write see details section
#'
#' @examples
#' \dontrun{
#' tmp <- tempfile(fileext = ".pdf")
#' writeLines("demo", tmp)
#' write_art_coa(
#'   "11111111-1111-1111-1111-111111111111",
#'   "22222222-2222-2222-2222-222222222222",
#'   tmp
#' )
#' }
#' @export
write_art_coa <- function(artist, artwork, local_path, delete_local = FALSE) {
  stopifnot("Invalid Artist UUID" = is.character(artist) && length(artist) == 1L)
  stopifnot("Invalid Artwork UUID" = is.character(artwork) && length(artwork) == 1L)

  bucket <- "art-coa"
  key <- stringr::str_glue("issued/{artist}/{artwork}/{fs::path_file(local_path)}")
  metadata <- list(artistID = artist, artworkID = artwork)

  # Since function writes single thumbnails, ensure path is not a dir
  stopifnot("Nothing Found at Local Path" = fs::file_exists(local_path))
  stopifnot("Dir upload not handled for art-coa" = fs::is_file(local_path))

  rdstools::log_inf(bucket, paste0("Uploading cert file => ", key))

  cdn_file_upload(
    bucket = bucket,
    key = key,
    local_path = local_path,
    metadata = metadata
  )
  if (delete_local) fs::file_delete(local_path)

  rdstools::log_suc("write_art_coa", paste0(bucket, " - ", key))

  invisible(key)
}

#' @describeIn cdn-write Upload a thumbnail to art-public
#'
#' @examples
#' \dontrun{
#' # 1) Artist thumbnail
#' thumb <- tempfile(fileext = ".png")
#' writeLines("png-data", thumb)
#' write_art_public("abcd1234-artist-uuid", local_path = thumb)
#'
#' # 2) Artwork thumbnail
#' thumb2 <- tempfile(fileext = ".jpeg")
#' writeLines("jpeg-data", thumb2)
#' write_art_public(
#'   "abcd1234-artist-uuid",
#'   "efgh5678-art-uuid",
#'   thumb2
#' )
#' }
#' @export
write_art_public <- function(artist, artwork = NULL, local_path, delete_local = FALSE) {
  if (is.null(artwork)) artwork <- ""

  # If artwork not provided, local_path is the artist's thumbnail,
  # else its the artwork thumbnail. These are the only two assets ever
  # written to the bucket via code. These should be jpeg, but keep flexible
  # until existing assets are converted from png to jpeg.
  #
  stopifnot("Invalid Artist UUID" = is.character(artist) && length(artist) == 1L)
  stopifnot("Invalid Artwork UUID" = is.character(artwork) && length(artwork) == 1L)

  # Since function writes single thumbnails, ensure path is not a dir
  stopifnot("Uploading folders not implemented for art-public" = fs::is_file(local_path))

  bucket <- "art-public"
  ext <- fs::path_ext(local_path)

  key <- ifelse(
    artwork == "",
    paste0("thumbnails/artist/", artist, ".", ext),
    paste0("thumbnails/", artist, "/", artwork, ".", ext)
  )
  metadata <- list(artistID = artist, artworkID = artwork) # artworkID can be NULL, filter in next line
  metadata <- Filter(function(x) !is.null(x) && nzchar(x), metadata)

  rdstools::log_inf(bucket, stringr::str_glue("Uploading thumb => {key}"))

  cdn_file_upload(bucket, key, local_path, metadata)

  if (delete_local) fs::file_delete(local_path)

  rdstools::log_suc(bucket, key)
  invisible(key)
}




# internal: verify every key is present in the bucket. used when local_path specifies a directory of files
.keys_exist <- function(bucket, keys) {
  s3 <- .get_s3()
  vapply(keys, function(k) {
    .object_exists(s3, bucket, k)
  }, logical(1L))
}



#' @describeIn cdn-write Upload processed assets to art-data
#'
#' @examples
#' \dontrun{
#' # single file
#' f <- tempfile(fileext = ".txt")
#' cat("hello", file = f)
#' write_art_data(
#'   "11111111-1111-1111-1111-111111111111",
#'   "22222222-2222-2222-2222-222222222222",
#'   local_path = f,
#'   prefix     = "processed/11111111-1111-1111-1111-111111111111/22222222-2222-2222-2222-222222222222"
#' )
#'
#' # directory of files
#' tmp <- tempfile("artdir_")
#' fs::dir_create(tmp)
#' cat("a", file = file.path(tmp, "a.txt"))
#' cat("b", file = file.path(tmp, "b.txt"))
#' write_art_data(
#'   "11111111-1111-1111-1111-111111111111",
#'   "22222222-2222-2222-2222-222222222222",
#'   local_path = tmp
#' )
#' }
#'
#' @export
write_art_data <- function(artist, artwork, local_path, prefix = NULL, delete_local = FALSE) {
  stopifnot("Invalid Artist UUID" = is.character(artist) & length(artist) == 1L)
  stopifnot("Invalid Artwork UUID" = is.character(artwork) & length(artwork) == 1L)

  ## This function should handle the case when local_path is a directory.
  ## Prefix should be the path to add keys to the bucket
  bucket <- "art-data"
  metadata <- list(artistID = artist, artworkID = artwork)
  metadata <- Filter(function(x) !is.null(x) && nzchar(x), metadata)

  # derive default prefix
  if (is.null(prefix)) {
    prefix <- stringr::str_glue("processed/{artist}/{artwork}")
  }
  prefix <- paste0(stringr::str_remove(prefix, "/$"), "/")

  ## In most cases, local_path should be a folder containing artwork files placed
  ## here: processed/{artist}/{artwork}/* (however this is not strictly enforced to
  ## allow for future scenario where objects can be added anytime AFTER
  ## authentication (not just once immediately upon authentication)
  ##
  stopifnot("Invalid Local Path" = fs::is_file(local_path) || fs::is_dir(local_path))
  stopifnot("Invalid Bucket Prefix" = is.character(prefix) & length(prefix) == 1L)

  rdstools::log_inf(stringr::str_glue("({bucket}) Upload to Prefix -> {prefix}"))

  ## Handle case when local_path is directory of files
  ##  - If local_path is directory, ensure prefix doesnt exist (has no objects)
  ##  - else it's single file so ensure overwriting is not allowed

  if (fs::is_dir(local_path)) {
    stopifnot("Cant Write to Prefix w/Existing Keys" = !has_prefix(bucket, prefix))

    files <- fs::dir_ls(local_path, all = TRUE, type = "file", recurse = TRUE)
    keys <- fs::path(prefix, fs::path_rel(files, local_path))
  } else {
    files <- local_path

    ## In case prefix was provided, ensure it is not a full key path with file name
    prefix_end <- fs::path_file(stringr::str_remove(prefix, "/$"))
    file_name <- fs::path_file(local_path)

    not_valid <- identical(prefix_end, file_name)

    # guard: prefix must NOT already end with the filename
    if (not_valid) {
      stop(
        "For single-file uploads, `prefix` must be a directory prefix, ",
        "not a full key.  Omit the file name; it is added automatically.",
        call. = FALSE
      )
    }
    keys <- fs::path(prefix, file_name)
  }

  rdstools::log_inf(bucket, stringr::str_glue("Write {length(files)} file(s) => {prefix}"))

  # upload each file ----------------------------------------------------------
  mapply(
    FUN = function(fp, k) cdn_file_upload(bucket, k, fp, metadata),
    files, keys,
    USE.NAMES = FALSE
  )

  # post-upload validation ----------------------------------------------------
  ok <- .keys_exist(bucket, keys)
  if (!all(ok)) {
    bad <- keys[!ok]
    rdstools::log_err("(art-data) Upload Aborted", stringr::str_glue("Missing {length(bad)} objects"))
    stop("Some objects failed to upload.  See log.", call. = FALSE)
  }

  # optional local cleanup ----------------------------------------------------
  if (isTRUE(delete_local)) {
    if (fs::is_dir(local_path)) {
      fs::dir_delete(local_path)
    } else {
      fs::file_delete(local_path)
    }
  }

  rdstools::log_suc(bucket, stringr::str_glue("Upload complete: {length(keys)} objects at {prefix}"))
  invisible(keys)
}

#' @describeIn cdn-write Upload an artist's original bundle to art-vault
#'
#' @examples
#' \dontrun{
#' dir_tmp <- fs::dir_create(tmp <- tempfile("vault_"))
#' writeLines("canvas", file.path(tmp, "canvas.procreate"))
#' write_art_vault("111...", "222...", tmp)
#' }
#' @export
write_art_vault <- function(artist, artwork, local_path, prefix = NULL, delete_local = FALSE) {
  stopifnot("Invalid Artist UUID" = is.character(artist) & length(artist) == 1L)
  stopifnot("Invalid Artwork UUID" = is.character(artwork) & length(artwork) == 1L)

  ## This function should handle the case when local_path is a directory.
  ## Prefix should be the path to add keys to the bucket
  bucket <- "art-vault"
  metadata <- list(artistID = artist, artworkID = artwork)
  metadata <- Filter(function(x) !is.null(x) && nzchar(x), metadata)

  ## If prefix not provided, prefix be set to default
  if (is.null(prefix)) {
    prefix <- stringr::str_glue("uploads/{artist}/{artwork}")
  }
  prefix <- paste0(stringr::str_remove(prefix, "/$"), "/") # clean trailing slash


  ## In most cases, local_path should be a folder containing a bundle of files uploaded
  ## by the artist and verified authentic by the platform. This function should NEVER
  ## overwrite, and for any given artwork, should only be run once immediately after
  ## artwork has been authenticated.
  stopifnot("Invalid Local Path" = fs::is_file(local_path) || fs::is_dir(local_path))
  stopifnot("Invalid Bucket Prefix" = is.character(prefix) & length(prefix) == 1L)

  rdstools::log_inf(stringr::str_glue("({bucket}) Upload to Prefix -> {prefix}"))

  ## Handle case when local_path is directory of files
  ##  - If local_path is directory, ensure prefix doesnt exist (has no objects)
  ##  - else it's single file so ensure overwriting is not allowed

  if (fs::is_dir(local_path)) {
    if (has_prefix(bucket, prefix)) {
      stop("Vault prefix already exists: ", prefix, call. = FALSE)
    }

    files <- fs::dir_ls(local_path, all = TRUE, type = "file", recurse = TRUE)
    keys <- fs::path(prefix, fs::path_rel(files, local_path))
  } else {
    files <- local_path
    keys <- fs::path(prefix, fs::path_file(local_path))
  }

  rdstools::log_inf(bucket, stringr::str_glue("Uploading {length(files)} file(s) => {prefix}"))

  # upload loop (abort on first failure)
  mapply(function(fp, k) {
    cdn_file_upload(bucket, k, fp, metadata)
  }, files, keys, USE.NAMES = FALSE)

  # post-upload validation
  ok <- .keys_exist(bucket, keys)
  if (!all(ok)) {
    bad <- keys[!ok]
    rdstools::log_err(
      bucket,
      stringr::str_glue("Upload incomplete - missing {length(bad)} objects")
    )
    stop("Some vault objects failed to upload.  See log.", call. = FALSE)
  }

  # optional local deletion
  if (delete_local) {
    ifelse(fs::is_dir(local_path), fs::dir_delete(local_path), fs::file_delete(local_path))
  }

  rdstools::log_suc(bucket, stringr::str_glue("Vault upload complete: {length(keys)} objects"))
  invisible(keys)
}





## 1. write_art* calls cdn_file_upload for each file+key to add to the bucket
## 2. cdn_file_upload will call the appropriate .upload_* function depending
## on size of file at local_path
##
cdn_file_upload <- function(bucket, key, local_path, metadata = NULL, part_size = 100 * 1024^2) {
  if (is.null(metadata)) metadata <- list()

  ## Check metadata
  stopifnot("Malformed metadata in cdn_upload" = is.list(metadata))

  ## Check local path
  stopifnot("No file found at local_path" = fs::is_file(local_path))

  ## Check local file and remote bucket key
  stopifnot("Unable to overwrite existing keys" = !has_object(bucket, key))

  s3 <- .get_s3() # from helpers file
  ct <- .guess_mime(local_path)

  if (fs::file_info(local_path)$size >= part_size) {
    res <- .upload_large(
      s3,
      bucket,
      key,
      local_path,
      metadata = metadata,
      part_size = part_size
    )
  } else {
    res <- .upload_small(
      s3 = s3,
      bucket = bucket,
      key = key,
      local_path = local_path,
      metadata = metadata
    )
  }

  ## quick post-upload validation
  if (!has_object(bucket, key)) {
    stop("Upload seemed to succeed but object not found: ", key, call. = FALSE)
  }

  invisible(key)
}



## For large files, use multi-part bucket uploading
##
.upload_large <- function(s3, bucket, key, local_path, metadata, part_size) {
  ## Filter out metadata that is NULL or ""
  metadata <- Filter(function(x) !is.null(x) && nzchar(x), metadata)

  # 1. Start the multipart upload
  mpu <- s3$create_multipart_upload(
    Bucket = bucket,
    Key = key,
    Metadata = metadata,
    ContentType = .guess_mime(local_path)
  )
  upload_id <- mpu$UploadId

  # 2. Slice file into parts of 'part_size' bytes
  fsize <- fs::file_info(local_path)$size
  n_parts <- ceiling(fsize / part_size)

  etags <- vector("list", n_parts)
  con <- file(local_path, "rb")

  for (part_number in seq_len(n_parts)) {
    rdstools::log_inf(bucket, paste0("Uploading Part ", part_number, " of ", n_parts))

    buf <- readBin(
      con,
      what = "raw",
      n = min(part_size, fsize - (part_number - 1) * part_size)
    )
    # 3. Upload each part
    res <- s3$upload_part(
      Bucket     = bucket,
      Key        = key,
      UploadId   = upload_id,
      PartNumber = part_number,
      Body       = buf
    )
    etags[[part_number]] <- list(ETag = res$ETag, PartNumber = part_number)
  }
  close(con)

  # 4. Complete the multipart upload
  ll <- s3$complete_multipart_upload(
    Bucket = bucket,
    Key = key,
    UploadId = upload_id,
    MultipartUpload = list(Parts = etags)
  )
  rdstools::log_suc(stringr::str_glue("({bucket}) Upload Complete"), ll$Key)

  return(ll)
}


## For small files, use regular whole-file bucket uploading
##
.upload_small <- function(s3, bucket, key, local_path, metadata) {
  rdstools::log_inf(bucket, stringr::str_glue("Uploading small object '{key}'"))
  file_size <- fs::file_info(local_path)$size
  body <- readBin(local_path, what = "raw", n = file_size)

  ## Filter out metadata that is NULL or ""
  metadata <- Filter(function(x) !is.null(x) && nzchar(x), metadata)

  res <- s3$put_object(
    Body = body,
    Bucket = bucket,
    Key = key,
    Metadata = metadata,
    ContentType = .guess_mime(local_path)
  )
  rdstools::log_suc(bucket, stringr::str_glue("Upload complete: '{key}' (ETag {res$ETag})"))
  res
}


# internal: very small MIME mapper used by .upload* functions to set ContentType
.guess_mime <- function(path) {
  ext <- tolower(tools::file_ext(path))
  switch(ext,
    png = "image/png",
    jpg = ,
    jpeg = "image/jpeg",
    gif = "image/gif",
    pdf = "application/pdf",
    # Procreate files:
    procreate = "application/x-procreate",
    "application/octet-stream" # fallback
  )
}
