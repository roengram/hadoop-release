package org.apache.hadoop.fs.azurenative;

/**
 * Indicates whether there are actual blobs indicating the existence
 * of directories or whether we're inferring their existence from them
 * having files in there.
 */
enum BlobMaterialization {
  /**
   * Indicates a directory that isn't backed by an actual blob,
   * but its existence is implied by the fact that there are files
   * in there. For example, if the blob /a/b exists then it implies
   * the existence of the /a directory if there's no /a blob indicating
   * it.
   */
  Implicit,
  /**
   * Indicates that the directory is backed by an actual blob that has the
   * isFolder metadata on it.
   */
  Explicit,
}
