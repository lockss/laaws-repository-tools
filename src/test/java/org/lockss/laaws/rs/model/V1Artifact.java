/*
 * Copyright (c) 2019, Board of Trustees of Leland Stanford Jr. University,
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
 * ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.lockss.laaws.rs.model;

import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.beans.Field;

import java.io.Serializable;
import java.util.Objects;

public class V1Artifact implements Serializable {
  private static final long serialVersionUID = 1961138745993115018L;

  @Field("id")
  private String id;

  @Field("collection")
  private String collection;

  @Field("auid")
  private String auid;

  @Field("uri")
  private String uri;

  @Field("version")
  private Integer version;

  @Field("committed")
  private Boolean committed;

  @Field("storageUrl")
  private String storageUrl;

  @Field("contentLength")
  private long contentLength;

  @Field("contentDigest")
  private String contentDigest;

  @Field("collectionDate")
  private long collectionDate;

  /**
   * Constructor. Needed by SolrJ for getBeans() support.
   */
  public V1Artifact() {
    // Intentionally left blank
  }

  public static V1Artifact fromArtifactSpec(ArtifactSpec spec) {
    V1Artifact artifact = new V1Artifact();

    artifact.id = spec.getArtifactId();

    artifact.setCollection(spec.getCollection());
    artifact.setAuid(spec.getAuid());
    artifact.setUri(spec.getUrl());
    artifact.setVersion(spec.getVersion());
    artifact.setCommitted(spec.isCommitted());
    artifact.setStorageUrl(spec.getStorageUrl());
    artifact.setContentLength(spec.getContentLength());
    artifact.setContentDigest(spec.getContentDigest());
    artifact.setCollectionDate(spec.getCollectionDate());

    return artifact;
  }

  public ArtifactIdentifier getIdentifier() {
    return new ArtifactIdentifier(id, collection, auid, uri, version);
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    if (StringUtils.isEmpty(collection)) {
      throw new IllegalArgumentException(
          "Cannot set null or empty collection");
    }
    this.collection = collection;
  }

  public String getAuid() {
    return auid;
  }

  public void setAuid(String auid) {
    if (StringUtils.isEmpty(auid)) {
      throw new IllegalArgumentException("Cannot set null or empty auid");
    }
    this.auid = auid;
  }

  public String getUri() {
    return uri;
  }

  public void setUri(String uri) {
    if (StringUtils.isEmpty(uri)) {
      throw new IllegalArgumentException("Cannot set null or empty URI");
    }
    this.uri = uri;
  }

  public Integer getVersion() {
    return version;
  }

  public void setVersion(Integer version) {
//        if (StringUtils.isEmpty(version)) {
//          throw new IllegalArgumentException(
//              "Cannot set null or empty version");
//        }
    if (version == null) {
      throw new IllegalArgumentException("Cannot set null version");
    }

    this.version = version;
  }

  public String getId() {
    return id;
  }

  public Boolean getCommitted() {
    return committed;
  }

  public void setCommitted(Boolean committed) {
    if (committed == null) {
      throw new IllegalArgumentException("Cannot set null commit status");
    }
    this.committed = committed;
  }

  public String getStorageUrl() {
    return storageUrl;
  }

  public void setStorageUrl(String storageUrl) {
    if (StringUtils.isEmpty(storageUrl)) {
      throw new IllegalArgumentException(
          "Cannot set null or empty storageUrl");
    }
    this.storageUrl = storageUrl;
  }

  public long getContentLength() {
    return contentLength;
  }

  public void setContentLength(long contentLength) {
    this.contentLength = contentLength;
  }

  public String getContentDigest() {
    return contentDigest;
  }

  public void setContentDigest(String contentDigest) {
    this.contentDigest = contentDigest;
  }

  /**
   * Provides the artifact collection date.
   *
   * @return a long with the artifact collection date in milliseconds since the
   *         epoch.
   */
  public long getCollectionDate() {
    return collectionDate;
  }

  /**
   * Saves the artifact collection date.
   *
   * @param collectionDate
   *          A long with the artifact collection date in milliseconds since the
   *          epoch.
   */
  public void setCollectionDate(long collectionDate) {
    this.collectionDate = collectionDate;
  }

  @Override
  public String toString() {
    return "V1Artifact{" +
        "id='" + id + '\'' +
        ", collection='" + collection + '\'' +
        ", auid='" + auid + '\'' +
        ", uri='" + uri + '\'' +
        ", version=" + version +
        ", committed=" + committed +
        ", storageUrl='" + storageUrl + '\'' +
        ", contentLength=" + contentLength +
        ", contentDigest='" + contentDigest + '\'' +
        ", collectionDate=" + collectionDate +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    V1Artifact artifact = (V1Artifact) o;
    return contentLength == artifact.contentLength &&
        collectionDate == artifact.collectionDate &&
        id.equals(artifact.id) &&
        collection.equals(artifact.collection) &&
        auid.equals(artifact.auid) &&
        uri.equals(artifact.uri) &&
        version.equals(artifact.version) &&
        committed.equals(artifact.committed) &&
        storageUrl.equals(artifact.storageUrl) &&
        contentDigest.equals(artifact.contentDigest);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, collection, auid, uri, version, committed, storageUrl, contentLength, contentDigest, collectionDate);
  }
}
