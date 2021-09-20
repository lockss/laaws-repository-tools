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

package org.lockss.laaws.rs.index.solr;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.index.IndexUpgrader;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.request.schema.FieldTypeDefinition;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.json.JSONException;
import org.json.JSONObject;
import org.lockss.laaws.rs.io.index.solr.SolrArtifactIndex;
import org.lockss.laaws.rs.io.index.solr.SolrQueryArtifactIterator;
import org.lockss.laaws.rs.io.index.solr.SolrResponseErrorException;
import org.lockss.laaws.rs.io.storage.local.LocalWarcArtifactDataStore;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.log.L4JLogger;
import org.lockss.util.ListUtil;
import org.lockss.util.io.FileUtil;

import java.io.*;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class SolrArtifactIndexAdmin {
  private final static L4JLogger log = L4JLogger.getLogger();

  /**
   * Default Solr core name for SolrArtifactIndex implementations.
   */
  public static final String DEFAULT_SOLRCORE_NAME = "lockss";

  /**
   * Latest version of the LOCKSS configuration set for {@link SolrArtifactIndex}.
   */
  public static final int LATEST_LOCKSS_CONFIGSET_VERSION = 3;

  /**
   * Target version of the Lucene index and segments.
   */
  public static final Version TARGET_LUCENE_VERSION = Version.fromBits(8, 9, 0);

  /**
   * The key in the custom user properties of the Solr core configuration overlay, used to track a Solr core's version
   * of its installed LOCKSS configuration set.
   */
  public static final String LOCKSS_CONFIGSET_VERSION_KEY = "lockss-configset-version";

  // COMMON ////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Creates a {@code Map} containing a field modifier.
   * <p>
   * See https://lucene.apache.org/solr/guide/6_6/updating-parts-of-documents.html#UpdatingPartsofDocuments-In-PlaceUpdates
   *
   * @param modifier A {@code String} specifying the modifier. Can be one of "set" or "inc".
   * @param value    A {@code Object} new value of the field, or number to increment a numeric field.
   * @return A {@code Map<String, Object>} containing the field modifier.
   */
  private static Map<String, Object> getFieldModifier(String modifier, Object value) {
    Map<String, Object> fieldModifier = new HashMap<>();
    fieldModifier.put(modifier, value);
    return fieldModifier;
  }

  /**
   * This static class contains SolrArtifactIndex reindex operations.
   */
  public static class SolrArtifactIndexReindex {

    /**
     * Reindexes artifacts in a Solr core or collection for a specific version of the LOCKSS provided Solr configuration
     * set.
     * <p>
     * Intended to be called by a loop over target versions. Does not iterative reindex operations up to target version!
     *
     * @param solrClient    A SolrJ {@code SolrCore} implementation.
     * @param targetVersion A {@code int} representing the target version of the reindex operation.
     */
    public static void reindexArtifactsForVersion(SolrClient solrClient, List<String> solrCredentials,
                                                  String collection, int targetVersion)
        throws SolrServerException, SolrResponseErrorException, IOException {

      log.trace("targetVersion = {}", targetVersion);

      try {

        switch (targetVersion) {
          case 2:
            reindexArtifactsFrom1To2(solrClient, solrCredentials, collection);
            break;

          case 3:
            reindexArtifactsFrom2To3(solrClient, solrCredentials, collection);
            break;

          default:
            throw new IllegalArgumentException("No post configset installation tasks for version: " + targetVersion);
        }

      } catch (IOException | SolrServerException | SolrResponseErrorException e) {
        log.error("Caught exception while performing post configset installation tasks [targetVersion: {}]", targetVersion);
        throw e;
      }
    }

    /**
     * Performs a reindex (atomic update) of artifacts between LOCKSS configuration set version 1 to 2.
     * <p>
     * Changes:
     * * The collectionDate field type changed from a Trie-based 'long' to Point-based 'plong'.
     * * Introduced a new sortUri field to influence correct sort order on an artifact's URI. This solution was chosen
     * over a Java CollationField or ICUCollationField for operation and administrative simplicity.
     *
     * @param solrClient A SolrJ {@code SolrCore} implementation.
     * @throws IOException
     * @throws SolrServerException
     * @throws SolrResponseErrorException
     */
    public static void reindexArtifactsFrom1To2(SolrClient solrClient, List<String> solrCredentials, String collection)
        throws IOException, SolrServerException, SolrResponseErrorException {

      try {
        // Loop through all the documents in the index.
        SolrQuery q = new SolrQuery().setQuery("*:*");

        for (Artifact artifact : IteratorUtils.asIterable(
            new SolrQueryArtifactIterator(collection, solrClient, solrCredentials, q))) {

          // Create a Solr input document
          SolrInputDocument document = new SolrInputDocument();

          // Specify the ID of the Solr document (artifact) to modify
          document.addField("id", artifact.getId());

          // This is for the new sortUri field
          document.addField("sortUri", getFieldModifier("set", artifact.getSortUri()));

          // This is for long to plong conversion
          document.addField("collectionDate", getFieldModifier("set", artifact.getCollectionDate()));

          log.trace("document = {}", document);

          // Create an update request to add the document
          UpdateRequest req = new UpdateRequest();
          req.add(document);
          req.setCommitWithin(-1);

          // Add Solr BasicAuth credentials if present
          if (solrCredentials != null) {
            req.setBasicAuthCredentials(
                /* Username */ solrCredentials.get(0),
                /* Password */ solrCredentials.get(1)
            );
          }

          // Add the document with the new field.
          handleSolrResponse(
              req.process(solrClient, collection),
              "Problem adding artifact '" + document + "' to Solr");
        }

        // Commit all changes
        UpdateRequest commitRequest = new UpdateRequest();
        commitRequest.setAction(UpdateRequest.ACTION.COMMIT, true, true);

        handleSolrResponse(
            commitRequest.process(solrClient, collection),
            "Problem committing changes to Solr");

      } catch (IOException | SolrServerException | SolrResponseErrorException e) {

        String errorMessage = "Caught exception while performing post configset installation tasks for version 2";
        log.error(errorMessage, e);
        throw e;

      }
    }

    private static void reindexArtifactsFrom2To3(SolrClient solrClient, List<String> solrCredentials, String collection)
        throws IOException, SolrServerException, SolrResponseErrorException {

      reindexAllArtifacts(solrClient, solrCredentials, collection);
    }

    /**
     * Experimental. Performs an in-place reindex of all artifacts in the Solr index. All fields must be "stored" in the
     * index's schema for this to work.
     */
    public static void reindexAllArtifacts(SolrClient solrClient, List<String> solrCredentials, String collection)
        throws SolrServerException, SolrResponseErrorException, IOException {

      try {
        // Match all documents
        SolrQuery q = new SolrQuery().setQuery("*:*");

        // Loop through all the documents in the index.
        for (Artifact artifact :
            IteratorUtils.asIterable(new SolrQueryArtifactIterator(collection, solrClient, solrCredentials, q))) {

          // Explicitly set artifact's URI field to update sortUri
          artifact.setUri(artifact.getUri());

          // UpdateRequest to add Solr document
          UpdateRequest req = new UpdateRequest();
          req.add(solrClient.getBinder().toSolrInputDocument(artifact));
          req.setCommitWithin(-1);

          handleSolrResponse(
              req.process(solrClient, collection),
              "Problem reindexing artifact [artifactId: " + artifact.getId() + "]"
          );
        }

        // Commit request
        UpdateRequest commitRequest = new UpdateRequest();
        commitRequest.setAction(UpdateRequest.ACTION.COMMIT, true, true);

        // Commit all changes
        handleSolrResponse(
            commitRequest.process(solrClient, collection),
            "Problem committing changes to Solr");

      } catch (IOException | SolrServerException | SolrResponseErrorException e) {
        log.error("Caught exception while performing an in-place reindex of all Solr documents: {}", e);
        throw e;
      }
    }
  }

  // SOLR STANDALONE (SOLRJ) ///////////////////////////////////////////////////////////////////////////////////////////


  /**
   * Determines whether a core exists in a Solr instance running in stand-alone mode by attempting to retrieve its
   * status.
   *
   * @param solrClient
   * @param coreName
   * @return
   * @throws IOException
   * @throws SolrServerException
   */
  public static boolean coreExists(SolrClient solrClient, String coreName) throws IOException, SolrServerException {
    CoreAdminResponse response = CoreAdminRequest.getStatus(coreName, solrClient);
    return response.getCoreStatus(coreName).size() > 0;
  }

  /**
   * Returns the names of available configuration sets on this Solr instance.
   *
   * @param solrClient A {@code SolrClient} instance to get the list of configuration set names from.
   * @return A {@code List<String>} containing the names of the configuration sets available.
   * @throws IOException
   * @throws SolrServerException
   */
  public List<String> getAvailableConfigSets(SolrClient solrClient) throws IOException, SolrServerException {
    ConfigSetAdminRequest.List listReq = new ConfigSetAdminRequest.List();
    return listReq.process(solrClient).getConfigSets();
  }

  /*
  public void miscCoreOperations(SolrClient solrClient) throws IOException, SolrServerException {
    // Try renaming the core
    CoreAdminRequest.renameCore("lockss-repo.old", "lockss-repo", solrClient);
    CoreAdminRequest.unloadCore("lockss-repo.new", true, true, solrClient);
    CoreAdminRequest.swapCore("lockss-repo", "lockss-repo.new", solrClient);
  }
  */

  // SCHEMA API UPDATER (SOLRJ) ////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Returns a Solr response unchanged, if it has a zero status. Throws, otherwise.
   *
   * @param solrResponse A SolrResponseBase with the Solr response tobe handled.
   * @param errorMessage A String with a custom error message to be included in
   *                     the thrown exception, if necessary.
   * @return a SolrResponseBase with the passed Solr response unchanged, if it
   * has a zero status.
   * @throws SolrResponseErrorException if the passed Solr response has a
   *                                    non-zero status.
   */
  static <T extends SolrResponseBase> T handleSolrResponse(T solrResponse,
                                                           String errorMessage) throws SolrResponseErrorException {
    log.debug2("solrResponse = {}", solrResponse);

    NamedList<Object> solrResponseResponse =
        (NamedList<Object>) solrResponse.getResponse();

    // Check whether the response does indicate success.
    if (solrResponse.getStatus() == 0
        && solrResponseResponse.get("error") == null
        && solrResponseResponse.get("errors") == null) {
      // Yes: Return the successful response.
      log.debug2("solrResponse indicates success");
      return solrResponse;
    }

    // No: Report the problem.
    log.trace("solrResponse indicates failure");

    SolrResponseErrorException snzse =
        new SolrResponseErrorException(errorMessage, solrResponse);
    log.error(snzse);
    throw snzse;
  }

  /**
   * This class is capable of updating the Solr schema of an instance of a Solr ArtifactIndex implementation using
   * Solr's Schema API.
   */
  @Deprecated
  public static class ArtifactIndexSchemaUpdater {

    // The prefix of the name of the field that holds the version of the ArtifactIndex Solr schema
    private static final String lockssSolrSchemaVersionFieldName = "solrSchemaLockssVersion";

    // Field type names (must match types defined in the Solr schema)
    private static final String solrIntegerType = "pint";
    private static final String solrLongType = "plong";

    // The fields defined in the Solr schema, indexed by their names.
    private Map<String, Map<String, Object>> solrSchemaFields = null;

    private SolrClient solrClient;
    private List<String> solrCredentials;

    public ArtifactIndexSchemaUpdater(SolrClient client, List<String> solrCredentials) {
      this.solrClient = client;
      this.solrCredentials = solrCredentials;
    }

    /**
     * Updates the schema, if necessary.
     *
     * @param targetSchemaVersion An int with the LOCKSS version of the Solr
     *                            schema to be updated.
     * @throws SolrResponseErrorException if Solr reports problems.
     * @throws SolrServerException        if Solr reports problems.
     * @throws IOException                if Solr reports problems.
     */
    private void updateSchema(String collection, int targetSchemaVersion)
        throws SolrResponseErrorException, SolrServerException, IOException {

      log.debug2("targetSchemaVersion = " + targetSchemaVersion);

      // Get the fields currently in the Solr schema
      solrSchemaFields = getSolrSchemaFields();

      // Find the current schema version
      int existingSchemaVersion = getExistingLockssSchemaVersion();
      log.trace("existingSchemaVersion = {}", existingSchemaVersion);

      // Check whether the existing schema is newer than what this version of the service expects
      if (targetSchemaVersion < existingSchemaVersion) {
        // Yes: Report the problem
        throw new RuntimeException("Existing LOCKSS Solr schema version is "
            + existingSchemaVersion
            + ", which is higher than the target schema version "
            + targetSchemaVersion
            + " for this service. Possibly caused by service downgrade.");
      }

      // Check whether the schema needs to be updated beyond the existing schema version
      if (targetSchemaVersion > existingSchemaVersion) {
        // Yes.
        log.trace(
            "Schema needs to be updated from version {} to version {}",
            existingSchemaVersion,
            targetSchemaVersion
        );

        // Update the schema and get the last updated version.
        int lastUpdatedVersion = updateSchema(collection, existingSchemaVersion, targetSchemaVersion);
        log.trace("lastRecordedVersion = {}", lastUpdatedVersion);

        log.info("Schema has been updated to LOCKSS version {}",
            lastUpdatedVersion);
      } else {
        // No.
        log.info("Schema is up-to-date at LOCKSS version {}",
            existingSchemaVersion);
      }

      log.debug2("Done.");
    }

    /**
     * Provides the definitions of the Solr schema fields.
     *
     * @return a Map<Map<String, Object>> with the definitions of the Solr schema
     * fields.
     * @throws SolrResponseErrorException if Solr reports problems.
     * @throws SolrServerException        if Solr reports problems.
     * @throws IOException                if Solr reports problems.
     */
    private Map<String, Map<String, Object>> getSolrSchemaFields()
        throws SolrResponseErrorException, SolrServerException, IOException {
      log.debug2("Invoked");
      SchemaResponse.FieldsResponse fieldsResponse = null;

      try {
        // Request the Solr schema fields.
        fieldsResponse =
            handleSolrResponse(new SchemaRequest.Fields().process(solrClient),
                "Problem getting Solr schema fields");
      } catch (SolrServerException | IOException e) {
        String errorMessage = "Exception caught getting Solr schema fields";
        log.error(errorMessage, e);
        throw e;
      }

      Map<String, Map<String, Object>> result = new HashMap<>();

      // Loop through all the fields.
      for (Map<String, Object> field : fieldsResponse.getFields()) {
        // Get the field name.
        String fieldName = (String) field.get("name");
        log.trace("fieldName = {}", fieldName);

        // Add the field to  the result.
        result.put(fieldName, field);
      }

      log.debug2("result = {}", result);
      return result;
    }

    /**
     * Provides the LOCKSS version of the current Solr schema.
     *
     * @return an int with the LOCKSS version of the current Solr schema.
     */
    private int getExistingLockssSchemaVersion() {
      log.debug2("Invoked");
      int result = getSolrSchemaLockssVersionFromField();
      log.trace("result = {}", result);

      // Check whether the schema is at version 1.
      if (result == 0 && hasSolrField("collection", "string")
          && hasSolrField("auid", "string")
          && hasSolrField("uri", "string")
          && hasSolrField("committed", "boolean")
          && hasSolrField("storageUrl", "string")
          && hasSolrField("contentLength", solrLongType)
          && hasSolrField("contentDigest", "string")
          && hasSolrField("version", solrIntegerType)
          && hasSolrField("collectionDate", "long")) {
        // Yes: The schema is at version 1.
        result = 1;
      }

      log.debug2("result = {}", result);
      return result;
    }

    /**
     * Provides an indication of whether a Solr schema field exists.
     *
     * @param fieldName A String with the name of the field.
     * @param fieldType A String with the type of the field.
     * @return a boolean with the indication.
     */
    private boolean hasSolrField(String fieldName, String fieldType) {
      Map<String, Object> field = solrSchemaFields.get(fieldName);

      return field != null
          && field.equals(getNewFieldAttributes(fieldName, fieldType, null));
    }

    /**
     * Provides the LOCKSS version of the current Solr schema from the Solr field.
     *
     * @return an int with the LOCKSS version of the current Solr schema.
     */
    private int getSolrSchemaLockssVersionFromField() {
      log.debug2("Invoked");
      int result = 0;

      // Get the Solr schema LOCKSS version field.
      Map<String, Object> field = solrSchemaFields.get(lockssSolrSchemaVersionFieldName);

      // Check whether it exists.
      if (field != null) {
        // Yes: Get the version.
        result = Integer.valueOf((String) field.get("default"));
      }

      log.debug2("result = {}", result);

      return result;
    }

    /**
     * Updates the schema to the target version.
     *
     * @param existingVersion An int with the existing schema version.
     * @param finalVersion    An int with the version of the schema to which the
     *                        schema is to be updated.
     * @return an int with the highest update version recorded in the schema.
     * @throws SolrResponseErrorException if Solr reports problems.
     * @throws SolrServerException        if Solr reports problems.
     * @throws IOException                if Solr reports problems.
     */
    private int updateSchema(String collection, int existingVersion, int finalVersion)
        throws SolrResponseErrorException, SolrServerException, IOException {

      log.debug2("existingVersion = {}", existingVersion);
      log.debug2("finalVersion = {}", finalVersion);

      int lastUpdatedVersion = existingVersion;

      // Loop through all the versions to be updated to reach the targeted
      // version.
      for (int from = existingVersion; from < finalVersion; from++) {
        log.trace("Updating from version {}...", from);

        // Perform the appropriate update of the Solr schema for this version.
        updateSchemaToVersion(collection, from + 1);

        // Remember the current schema version.
        lastUpdatedVersion = from + 1;
        log.debug("Solr Schema updated to LOCKSS version {}", lastUpdatedVersion);

        // Record it in the field with the schema version.
        updateSolrLockssSchemaVersionField(lastUpdatedVersion);
      }

      log.debug2("lastRecordedVersion = {}", lastUpdatedVersion);
      return lastUpdatedVersion;
    }

    /**
     * Updates the Solr schema to a given LOCKSS version.
     *
     * @param targetVersion An int with the LOCKSS version of the schema to which
     *                      the Solr schema is to be updated.
     * @throws SolrResponseErrorException if Solr reports problems.
     * @throws SolrServerException        if Solr reports problems.
     * @throws IOException                if Solr reports problems.
     */
    private void updateSchemaToVersion(String collection, int targetVersion)
        throws SolrResponseErrorException, SolrServerException, IOException {

      log.debug2("targetVersion = {}", targetVersion);

      // Perform the appropriate Solr schema update for this version.
      try {
        switch (targetVersion) {
          case 1:
            updateSchemaFrom0To1();
            break;
          case 2:
            updateSchemaFrom1To2(collection);
            break;
          default:
            throw new IllegalArgumentException("Non-existent method to update the schema to version " + targetVersion);
        }
      } catch (SolrServerException | IOException e) {
        String errorMessage = "Exception caught updating Solr schema to LOCKSS version " + targetVersion;
        log.error(errorMessage, e);
        throw e;
      }
    }

    /**
     * Creates a Solr field of the given name and type, that is indexed, stored, required but not multivalued.
     *
     * @param solr      A {@code SolrClient} that points to a Solr core or collection to add the field to.
     * @param fieldName A {@code String} containing the name of the new field.
     * @param fieldType A {@code String} containing the name of the type of the new field.
     * @throws SolrResponseErrorException if Solr reports problems.
     * @throws SolrServerException        if Solr reports problems.
     * @throws IOException                if Solr reports problems.
     */
    private void createSolrField(SolrClient solr, String fieldName, String fieldType)
        throws SolrResponseErrorException, IOException, SolrServerException {
      createSolrField(solr, fieldName, fieldType, null);
    }

    /**
     * Creates a Solr field of the given name and type, that is indexed, stored, required but not multivalued.
     * <p>
     * Additional field attributes can be provided, or default attributes can be overridden, by passing field
     * attributes.
     *
     * @param solr            A {@code SolrClient} that points to a Solr core or collection to add the field to.
     * @param fieldName       A {@code String} containing the name of the new field.
     * @param fieldType       A {@code String} containing the name of the type of the new field.
     * @param fieldAttributes A {@code Map<String, Object>} containing additional field attributes, and/or a map of
     *                        fields to override.
     * @throws SolrResponseErrorException if Solr reports problems.
     * @throws SolrServerException        if Solr reports problems.
     * @throws IOException                if Solr reports problems.
     */
    private void createSolrField(SolrClient solr, String fieldName, String fieldType, Map<String, Object> fieldAttributes)
        throws SolrResponseErrorException, IOException, SolrServerException {
      // https://lucene.apache.org/solr/guide/7_2/defining-fields.html#DefiningFields-OptionalFieldTypeOverrideProperties
      Map<String, Object> newFieldAttributes =
          getNewFieldAttributes(fieldName, fieldType, fieldAttributes);

      // Only create the field if it does not exist
      if (!hasSolrField(fieldName, fieldType)) {
        // Create and process new field request
        log.debug("Adding field to Solr schema: {}", newFieldAttributes);
        SchemaRequest.AddField addFieldReq = new SchemaRequest.AddField(newFieldAttributes);
        handleSolrResponse(addFieldReq.process(solr),
            "Problem adding field to Solr schema");
      } else {
        log.debug("Field already exists in Solr schema: {}; skipping field addition", newFieldAttributes);
      }
    }

    /**
     * Provides the attributes of a new field.
     *
     * @param fieldName       A String with the name of the field.
     * @param fieldType       A String with the type of the field.
     * @param fieldAttributes A Map<String,Object> with overriding field
     *                        attributes, if any.
     * @return a Map<String, Object> with the attributes of the new field.
     */
    private Map<String, Object> getNewFieldAttributes(String fieldName,
                                                      String fieldType, Map<String, Object> fieldAttributes) {
      // https://lucene.apache.org/solr/guide/7_2/defining-fields.html#DefiningFields-OptionalFieldTypeOverrideProperties
      Map<String, Object> newFieldAttributes = new LinkedHashMap<>();
      newFieldAttributes.put("name", fieldName);
      newFieldAttributes.put("type", fieldType);
      newFieldAttributes.put("indexed", true);
      newFieldAttributes.put("stored", true);
      newFieldAttributes.put("multiValued", false);
      newFieldAttributes.put("required", true);

      // Allow default attributes to be overridden if field attributes were
      // provided.
      if (fieldAttributes != null) {
        newFieldAttributes.putAll(fieldAttributes);
      }

      return newFieldAttributes;
    }

    /**
     * Creates a new Solr field type, provided the new field type's attributes. Does not support setting an analyzer.
     *
     * @param fieldTypeAttributes A {@code Map<String, Object>} containing attributes of the field type to create.
     * @return An {@code UpdateResponse} from Solr.
     * @throws IOException         if Solr reports problems.
     * @throws SolrServerException if Solr reports problems.
     */
    private SchemaResponse.UpdateResponse createSolrFieldType(Map<String, Object> fieldTypeAttributes)
        throws IOException, SolrServerException {
      // Create new field type definition
      FieldTypeDefinition ftd = new FieldTypeDefinition();
      ftd.setAttributes(fieldTypeAttributes);

      // Create and submit new add-field-type request
      SchemaRequest.AddFieldType req = new SchemaRequest.AddFieldType(ftd);
      return req.process(solrClient);
    }

    private void updateSchemaFrom0To1() throws SolrServerException, IOException, SolrResponseErrorException {
      // New field type definition for "long" (using depreciated TrieLongField)
      // <fieldType name="long" class="solr.TrieLongField" docValues="true" precisionStep="0" positionIncrementGap="0"/>
      Map<String, Object> ftd = new HashMap<>();
      ftd.put("name", "long");
      ftd.put("class", "solr.TrieLongField");
      ftd.put("docValues", "true");
      ftd.put("precisionStep", 0);
      ftd.put("positionIncrementGap", 0);

      // Create "long" field type if it does not exist
      if (!hasSolrFieldType(ftd)) {
        createSolrFieldType(ftd);
      }

      // Create initial set of fields
      createSolrField(solrClient, "collection", "string");
      createSolrField(solrClient, "auid", "string");
      createSolrField(solrClient, "uri", "string");
      createSolrField(solrClient, "committed", "boolean");
      createSolrField(solrClient, "storageUrl", "string");
      createSolrField(solrClient, "contentLength", solrLongType);
      createSolrField(solrClient, "contentDigest", "string");
      createSolrField(solrClient, "version", solrIntegerType);
      createSolrField(solrClient, "collectionDate", "long");
    }

    /**
     * Returns a boolean indicating whether a Solr field type matching the provided field type attributes exists in the
     * Solr schema.
     *
     * @param fieldTypeAttributes A {@code Map<String, Object>} containing attributes of the field type to search for.
     * @return A {@code boolean} indicating whether the field type exists in the Solr schema.
     * @throws IOException         if Solr reports problems.
     * @throws SolrServerException if Solr reports problems.
     */
    private boolean hasSolrFieldType(Map<String, Object> fieldTypeAttributes) throws IOException, SolrServerException {
      SchemaRequest.FieldTypes req = new SchemaRequest.FieldTypes();
      SchemaResponse.FieldTypesResponse res = req.process(solrClient);

      return res.getFieldTypes().stream().anyMatch(ft -> ft.equals(fieldTypeAttributes));
    }

    /**
     * Updates the Solr schema from LOCKSS version 1 to 2.
     *
     * @throws SolrResponseErrorException if Solr reports problems.
     * @throws SolrServerException        if Solr reports problems.
     * @throws IOException                if Solr reports problems.
     */
    private void updateSchemaFrom1To2(String collection)
        throws SolrResponseErrorException, SolrServerException, IOException {
      log.debug2("Invoked");

      // Replace collectionDate field definition with one that uses field type "plong"
      SchemaRequest.ReplaceField req = new SchemaRequest.ReplaceField(
          getNewFieldAttributes("collectionDate", solrLongType, null)
      );
      req.process(solrClient);

      // Remove "long" field type definition
//    SchemaRequest.DeleteFieldType delReq = new SchemaRequest.DeleteFieldType("long");
//    delReq.process(solrClient);

      // Create the new field in the schema.
      createSolrField(solrClient, "sortUri", "string");

      try {
        // Loop through all the documents in the index.
        SolrQuery q = new SolrQuery().setQuery("*:*");

        for (Artifact artifact :
            IteratorUtils.asIterable(new SolrQueryArtifactIterator(collection, solrClient, solrCredentials, q))) {

          // Initialize a document with the artifact identifier.
          SolrInputDocument document = new SolrInputDocument();
          document.addField("id", artifact.getId());

          // Add the new field value.
          document.addField("sortUri", getFieldModifier("set", artifact.getSortUri()));
          document.addField("collectionDate", getFieldModifier("set", artifact.getCollectionDate()));
          log.trace("document = {}", document);

          // UpdateRequest to add Solr document
          UpdateRequest updateRequest = new UpdateRequest();
          updateRequest.add(solrClient.getBinder().toSolrInputDocument(artifact));
          updateRequest.setCommitWithin(-1);

          // Add the document with the new field.
          handleSolrResponse(solrClient.add(document), "Problem adding document '" + document + "' to Solr");
        }

        // Commit request
        UpdateRequest commitRequest = new UpdateRequest();
        commitRequest.setAction(UpdateRequest.ACTION.COMMIT, true, true);

        // Commit all changes
        handleSolrResponse(
            commitRequest.process(solrClient, collection),
            "Problem committing changes to Solr");

      } catch (SolrServerException | IOException e) {
        String errorMessage = "Exception caught updating Solr schema to LOCKSS version 2";
        log.error(errorMessage, e);
        throw e;
      }

      log.debug2("Done");
    }

    /**
     * Updates the Solr schema LOCKSS version field in the index.
     *
     * @param schemaVersion An int with the LOCKSS version of the Solr schema.
     * @throws SolrResponseErrorException if Solr reports problems.
     * @throws SolrServerException        if Solr reports problems.
     * @throws IOException                if Solr reports problems.
     */
    public void updateSolrLockssSchemaVersionField(int schemaVersion)
        throws SolrResponseErrorException, SolrServerException, IOException {
      log.debug2("schemaVersion = {}", schemaVersion);

      Map<String, Object> newFieldAttributes = new LinkedHashMap<>();
      newFieldAttributes.put("name", lockssSolrSchemaVersionFieldName);
      newFieldAttributes.put("type", solrIntegerType);
      newFieldAttributes.put("indexed", true);
      newFieldAttributes.put("stored", true);
      newFieldAttributes.put("multiValued", false);
      newFieldAttributes.put("required", false);
      newFieldAttributes.put("default", String.valueOf(schemaVersion));

      try {
        // Get the Solr schema LOCKSS version field.
        Map<String, Object> field =
            solrSchemaFields.get(lockssSolrSchemaVersionFieldName);
        log.trace("field = {}", field);

        // Check whether it exists.
        if (field != null) {
          // Yes: Replace the existing field.
          log.trace("Replacing field '{}' in Solr schema", field);
          handleSolrResponse(new SchemaRequest.ReplaceField(newFieldAttributes)
                  .process(solrClient),
              "Problem replacing field '" + field + "' in the Solr schema");
        } else {
          // No: Add the field for the Solr schema LOCKSS version.
          log.trace("Adding field to Solr schema: {}", newFieldAttributes);
          handleSolrResponse(new SchemaRequest.AddField(newFieldAttributes)
                  .process(solrClient),
              "Problem adding field '" + field + "' in the Solr schema");
        }
      } catch (SolrServerException | IOException e) {
        String errorMessage = "Exception caught updating Solr schema LOCKSS "
            + "version field to " + schemaVersion;
        log.error(errorMessage, e);
        throw e;
      }

      log.debug2("Done");
    }
  }

  // LOCAL /////////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * This class contains operations necessary to administrate a Solr core on a local filesystem.
   */
  public static class LocalSolrCoreAdmin {

    public static final String CONFIGOVERLAY_FILE = "configoverlay.json";
    public static final String CONFIGOVERLAY_USERPROPS_KEY = "userProps";

    /**
     * The name of the Solr core. By default this is the name of the directory containing the {@code core.properties} of
     * the core, but the name can be specified in the properties file by setting the {@code name} field.
     */
    protected String solrCoreName;

    /**
     * The base path to the Solr home directory containing the core.
     * <p>
     * It should contain a {@code solr.xml} configuration file for the Solr server (or node, if running as part of a
     * Solr Cloud cluster). The path can also be used to instantiate an {@code EmbeddedSolrServer} or
     * {@core CoreContainer}.
     */
    protected Path solrHome;

    /**
     * Base path of the Solr core.
     */
    protected Path instanceDir;

    /**
     * The path to the core's configuration set.
     * <p>
     * The core's configuration set is in the {@code conf/} directory under the Solr core's instance directory. If the
     * name of a shared configuration set is provided (by setting the {@code configSet} property in
     * {@code core.properties}) then this points to the path of the configuration set in the shared configuration set
     * base directory.
     */
    protected Path configDirPath;

    /**
     * The path to the core's data directory.
     * <p>
     * By default, this is the {@code data/} directory under the Solr core's instance directory, unless specified by
     * setting the {@code dataDir} property in {@code core.properties}.
     */
    protected Path dataDir;

    /**
     * The path to the core's Lucene index.
     * <p>
     * By default, this is the {@code index/} directory under the core's data directory.
     */
    protected Path indexDir;

    /**
     * The name of a shared configuration set used by the core.
     */
    protected String sharedConfigSetName;

    /**
     * The path to the shared configuration set base directory.
     * <p>
     * By default, this is the {@code configsets/} directory under the Solr home directory, unless configured in
     * {@code solr.xml}.
     */
    protected Path sharedConfigSetBaseDir;

    /**
     * An integer representing the version of the LOCKSS Solr configuration set installed in this core.
     */
    protected int lockssConfigSetVersion;

    /**
     * Constructor. Takes individual Solr core configuration parameters needed to perform an update of the Solr core.
     *
     * @param name                   A {@code String} containing the name of the Solr core.
     * @param solrHome               A {@code Path} to the Solr home base directory that contains the core.
     * @param instanceDir            A {@code Path} to the Solr core instance directory.
     * @param configDirPath          A {@code Path} to the Solr core's configuration set directory.
     * @param dataDir                A {@code Path} to the Solr core's data directory.
     * @param indexDir               A {@code Path} to the Solr core's Lucene index directory.
     * @param sharedConfigSetName    A {@code String} containing the name of the shared configuration set used by the core.
     * @param sharedConfigSetBaseDir A {@code Path} to the shared configuration set base directory.
     * @param version                An {@code int} containing the version of the LOCKSS configuration set installed in the core.
     */
    public LocalSolrCoreAdmin(
        String name,
        Path solrHome,
        Path instanceDir,
        Path configDirPath,
        Path dataDir,
        Path indexDir,
        String sharedConfigSetName,
        Path sharedConfigSetBaseDir,
        int version
    ) {
      this.solrCoreName = name;
      this.solrHome = solrHome;
      this.instanceDir = instanceDir;
      this.configDirPath = configDirPath;
      this.dataDir = dataDir;
      this.indexDir = indexDir;
      this.sharedConfigSetName = sharedConfigSetName;
      this.sharedConfigSetBaseDir = sharedConfigSetBaseDir;
      this.lockssConfigSetVersion = version;
    }

    /**
     * Creates a new Solr core using the parameters of this {@code LocalSolrCoreAdmin}.
     *
     * @throws IOException
     * @throws SolrServerException
     */
    public void create() throws IOException, SolrServerException {
      if (configDirPath == null) {
        throw new IllegalStateException("Cannot create Solr core with a null configuration directory path");
      }

      // Create the core instance directory
      FileUtil.ensureDirExists(configDirPath.toFile());

      // Install configuration set
      installLockssConfigSetVersion(lockssConfigSetVersion);

      // Start embedded Solr server and send create core request
      try (EmbeddedSolrServer solrClient = new EmbeddedSolrServer(solrHome, solrCoreName)) {
        CoreAdminRequest.Create req = new CoreAdminRequest.Create();

        req.setCoreName(solrCoreName);
        req.setInstanceDir(solrHome.relativize(instanceDir).toString());

        req.process(solrClient);
      }
    }

    /**
     * Creates a new Solr core under the provided Solr home base directory with the latest LOCKSS configuration set
     * version.
     *
     * @param solrHome     A {@code Path} containing the path to a Solr home base directory.
     * @param solrCoreName A {@code String} containing the name of the Solr core to create.
     * @throws IOException
     * @throws SolrServerException
     */
    public static void createCore(Path solrHome, String solrCoreName) throws IOException, SolrServerException {
      createCore(solrHome, solrCoreName, LATEST_LOCKSS_CONFIGSET_VERSION);
    }

    /**
     * Creates a new Solr core under the provided Solr home base directory with a specified version of the LOCKSS
     * configuration set version.
     *
     * @param solrHome               A {@code Path} containing the path to a Solr home base directory.
     * @param solrCoreName           A {@code String} containing the name of the Solr core to create.
     * @param lockssConfigSetVersion An {@code int} containing the version of the LOCKSS configuration set to install.
     * @throws IOException
     * @throws SolrServerException
     */
    public static void createCore(Path solrHome, String solrCoreName, int lockssConfigSetVersion) throws IOException, SolrServerException {
      if (lockssConfigSetVersion <= 0) {
        throw new IllegalArgumentException("Illegal LOCKSS configuration set version [version: " + lockssConfigSetVersion + "]");
      }

      // Solr core instance directory
      Path instanceDir = solrHome.resolve(String.format("lockss/cores/%s", solrCoreName));

      // Instantiate a new local Solr core admin
      LocalSolrCoreAdmin admin = new LocalSolrCoreAdmin(
          solrCoreName,
          solrHome,
          instanceDir,

          // The following three paths mimic default Solr behavior
          instanceDir.resolve("conf"),
          instanceDir.resolve("data"),
          instanceDir.resolve("data/index"),

          // Shared configuration set not used here
          null,
          null,

          // Config set version
          lockssConfigSetVersion
      );

      // Create the Solr core
      admin.create();
    }

    /**
     * Performs all steps necessary to bring the underlying Solr core up-to-date.
     */
    public void update() throws IOException, SolrResponseErrorException, SolrServerException {
      File reindexLockFile = indexDir.resolve(UPGRADE_LOCK_FILE).toFile();

      try (FileChannel channel = new RandomAccessFile(reindexLockFile, "rw").getChannel()) {
        try (FileLock lock = channel.tryLock()) {
          upgradeLuceneIndex();
          updateConfigSet();
        } catch (OverlappingFileLockException e) {
          // Another thread has the lock
          log.info("An update is already in progress");
        }
      }
    }

    /**
     * Convenience method. Returns a {@code boolean} indicating whether this Solr core has an update available for its
     * Lucene index and segments, or its LOCKSS configuration set.
     *
     * @return A {@code boolean} indicating whether this Solr core has an update available for its Lucene index and
     * segments, or its LOCKSS configuration set.
     */
    public boolean isUpdateAvailable() throws IOException {
      return isLuceneIndexUpgradeAvailable() || isLockssConfigSetUpdateAvailable();
    }

    /**
     * Returns a {@code boolean} indicating whether the Solr core exists on disk under the Solr home base directory.
     * Uses a {@link CoreContainer} to discover Solr cores under the Solr home path.
     *
     * @return A {@code boolean} indicating whether the Solr core exists under the Solr home base directory.
     */
    public boolean isCoreExists() {
      CoreContainer container = CoreContainer.createAndLoad(solrHome);

      try {
        boolean result = container.getAllCoreNames().contains(solrCoreName);
        log.trace("isExists(core = {}) = {}", getCoreName(), result);
        return result;
      } finally {
        container.shutdown();
      }
    }

    /**
     * Returns a {@code boolean} indicating whether an upgrade is available for the Lucene index (and its segments) in
     * this core. Determined by comparing its oldest segment version with the target version.
     *
     * @return A {@code boolean} indicating whether the oldest segment in the Lucene index is below the target version.
     * @throws IOException Thrown if an error occurred while reading the core's Lucene segments.
     */
    public boolean isLuceneIndexUpgradeAvailable() throws IOException {
      boolean result;

      // Get the minimum version of all the committed segments in this Lucene index
      SegmentInfos segInfos = getSegmentInfos();

      log.trace("segInfos.size() = {}", segInfos.size());

      // Index must have at least one segment
      if (segInfos.size() > 0) {
        // Get version of the oldest segment in this index
        Version minSegVersion = segInfos.getMinSegmentLuceneVersion();

        log.trace("minSegVersion = {}, targetVersion = {}", minSegVersion, TARGET_LUCENE_VERSION);

        result = !minSegVersion.onOrAfter(TARGET_LUCENE_VERSION);
      } else {
        log.trace("Index contains no segments");
        result = false;
      }

      log.trace("isLuceneIndexUpgradeAvailable(core = {}) = {}", getCoreName(), result);
      return result;
    }

    /**
     * Returns a {@code boolean} indicating whether a new LOCKSS configuration set is available for this core.
     * Determined by comparing the current LOCKSS configuration set version with the latest version.
     * See {@link LocalSolrCoreAdmin#getLockssConfigSetVersion()} for details.
     *
     * @return Returns a {@code boolean} indicating whether a new LOCKSS configuration set is available for this core.
     * @throws IOException Thrown if an error occurs while attempting to read the Solr Configuration Overlay file.
     */
    public boolean isLockssConfigSetUpdateAvailable() throws IOException {
      boolean result = getLockssConfigSetVersion() < LATEST_LOCKSS_CONFIGSET_VERSION;
      log.trace("isLockssConfigSetUpdateAvailable(core = {}) = {}", getCoreName(), result);
      return result;
    }

    /**
     * Returns a {@code boolean} indicating whether an update is in progress for this core. Determined by whether we're
     * able to acquire the update lock at a given instance.
     *
     * @return Returns a {@code boolean} indicating whether an update is in progress.
     * @throws IOException Thrown if there were I/O problems with the lock file.
     */
    public boolean isUpdateInProgress() throws IOException {
      boolean result;

      File lockfile = indexDir.resolve(UPGRADE_LOCK_FILE).toFile();

      log.trace("lockfile = {}", lockfile);

      try (FileChannel channel = new RandomAccessFile(lockfile, "rw").getChannel()) {
        try (FileLock lock = channel.tryLock()) {
          // Acquired lock - an update was not in progress in another thread
          result = false;
        } catch (OverlappingFileLockException e) {
          // Could not acquire lock - we interpret this to mean another thread is running the update
          result = true;
        }
      }

      log.trace("isUpdateInProgress(core = {}) = {}", getCoreName(), result);
      return result;
    }

    /**
     * Returns a {@code boolean} indicating whether a reindex is in progress. Determined by whether the reindex lockfile
     * is present in this core.
     * <p>
     * This *not* thread-safe!
     *
     * @return Returns a {@code boolean} indicating whether a reindex is in progress.
     */
    public boolean isReindexInProgress() throws IOException {
      File reindexLockFile = indexDir.resolve(REINDEX_LOCK_FILE).toFile();
      return reindexLockFile.exists();
    }

    /**
     * Applies schema updates to this Solr core using the Schema API through SolrJ.
     */
    @Deprecated
    public void applySchemaUpdates() {
      log.debug("Applying schema updates through SolrJ");

      try (SolrClient solrClient = new EmbeddedSolrServer(solrHome, solrCoreName)) {
        new ArtifactIndexSchemaUpdater(solrClient, null)
            .updateSchema(getCoreName(), LATEST_LOCKSS_CONFIGSET_VERSION);
      } catch (IOException | SolrServerException | SolrResponseErrorException e) {
        // TODO better error handling
        e.printStackTrace();
      }
    }

    /**
     * Performs an upgrade of the Solr core's Lucene index if necessary.
     *
     * @throws IOException
     */
    public void upgradeLuceneIndex() throws IOException {
      if (isLuceneIndexUpgradeAvailable()) {
        new IndexUpgrader(FSDirectory.open(indexDir)).upgrade();
      }
    }

    /**
     * Returns a {@code SegmentInfos} containing segment information for all segments in this Lucene index.
     *
     * @return A {@code SegmentInfos} containing segment information for all segments in this Lucene index.
     * @throws IOException
     */
    public SegmentInfos getSegmentInfos() throws IOException {
      if (indexDir == null) {
        throw new IllegalStateException("Null index directory path");
      }

      return SegmentInfos.readLatestCommit(FSDirectory.open(indexDir));
    }

    /**
     * Updates the configuration set of the LOCKSS repository Solr core iteratively to the latest version, and performs
     * any post-update Solr document reindexing.
     *
     * @throws IOException
     * @throws SolrResponseErrorException
     * @throws SolrServerException
     */
    public void updateConfigSet() throws IOException, SolrResponseErrorException, SolrServerException {
      if (!isLockssConfigSetUpdateAvailable()) {
        log.trace("Already at latest; nothing to do");
        return;
      }

      int targetVersion = LATEST_LOCKSS_CONFIGSET_VERSION;

      if (lockssConfigSetVersion <= 0) {
        // Yes: Invalid configuration set version: Attempt to ready from configuration overlay file.
        lockssConfigSetVersion = getLockssConfigSetVersion();
      }

      log.trace("currentVersion = {}", lockssConfigSetVersion);
      log.trace("targetVersion = {}", targetVersion);

      // Retire the existing, production configuration set
      retireConfigSet();

      // Update index to target version iteratively
      for (int version = lockssConfigSetVersion; version < targetVersion; version++) {

        // Remove configuration set from previous iteration if it exists
        FileUtils.deleteDirectory(configDirPath.toFile());

        // Install configuration set
        installLockssConfigSetVersion(version + 1);

        // Perform post-installation tasks. We start/stop the server each iteration to avoid config sets changing from
        // under the embedded Solr server and causing unpredictable behavior.
        try (EmbeddedSolrServer solrClient = new EmbeddedSolrServer(solrHome, solrCoreName)) {

          File reindexLockFile = indexDir.resolve(REINDEX_LOCK_FILE).toFile();

          if (!reindexLockFile.exists()) {
            // Acquired reindex lock
            FileUtils.touch(reindexLockFile);
            SolrArtifactIndexReindex.reindexArtifactsForVersion(solrClient, null, getCoreName(), version + 1);
            FileUtils.forceDelete(reindexLockFile);
          } else {
            // Could not acquire reindex lock (reindex lock file already exists)
            log.trace("Reindex is already in progress");
            return;
          }

//          try (FileChannel channel = new RandomAccessFile(reindexLockFile, "rw").getChannel()) {
//            try (FileLock lock = channel.tryLock()) {
//              SolrArtifactIndexReindex.reindexArtifactsForVersion(solrClient, version + 1);
//            }
//          }

        } catch (SolrServerException | SolrResponseErrorException e) {
          log.error(
              "Caught an exception while attempting to reindex artifacts for target version [version: {}]: {}",
              version + 1, e
          );

          throw e;
        }

        // Increment LOCKSS config set version of this Solr core
        lockssConfigSetVersion++;
      }
    }

    public static final String UPGRADE_LOCK_FILE = "upgrade.lock";
    public static final String REINDEX_LOCK_FILE = "reindex.lock";

    /**
     * Determines the version of the LOCKSS Solr configuration set in this core by reading its configuration overlay
     * JSON file directly and returning the value of the "lockss-configset-version" key from userProps.
     * <p>
     * Returns 0 if the configuration overlay file could not be found, or if the LOCKSS configuration set version key
     * does not exist in the overlay.
     *
     * @return An {@code int} containing the version of the configuration set.
     * @throws IOException
     */
    public int getLockssConfigSetVersion() throws IOException {
      try {
        return getLockssConfigSetVersionFromOverlay(configDirPath);
      } catch (FileNotFoundException e) {
        // If the configuration overlay file does not exist then try the "alpha1" method, where the LOCKSS configuration
        // set version was recorded as a default value for the "solrSchemaLockssVersion" field in the Solr index schema.
        return getLockssConfigSetVersionFromField(solrHome, solrCoreName);
      }
    }

    /**
     * Determines the version of the LOCKSS Solr configuration set installed at a path by reading the configuration
     * overlay JSON file and returning the value of the "lockss-configset-version" key from userProps.
     * <p>
     * Returns 0 if the configuration overlay file could not be found at the path, or if the LOCKSS configuration set
     * version key does not exist in the overlay.
     *
     * @param configDir A {@link Path} containing the path to a configuration set.
     * @return An {@code int} containing the version of the configuration set.
     * @throws FileNotFoundException If configuration overlay file does not exist.
     */
    public static int getLockssConfigSetVersionFromOverlay(Path configDir) throws IOException {
      if (Objects.isNull(configDir)) {
        throw new IllegalArgumentException("Null configuration set path");
      }

      File configOverlayPath = configDir.resolve(CONFIGOVERLAY_FILE).toFile();

      // Read JSON file
      try (InputStream input = new FileInputStream(configOverlayPath)) {
        JSONObject json = new JSONObject(IOUtils.toString(input, "UTF-8"));

        if (json.has(CONFIGOVERLAY_USERPROPS_KEY)) {
          JSONObject userProps = json.getJSONObject(CONFIGOVERLAY_USERPROPS_KEY);

          if (userProps.has(LOCKSS_CONFIGSET_VERSION_KEY)) {
            return userProps.getInt(LOCKSS_CONFIGSET_VERSION_KEY);
          }
        }
      } catch (JSONException e) {
        log.error("JSON parser exception while reading Solr configuration overlay [cfgPath: {}]", configOverlayPath, e);
        throw new IOException("Error reading Solr configuration overlay file", e);
      }

      return 0;
    }

    /**
     * Depreciated. The field in the Solr schema used to record the LOCKSS configuration set version.
     */
    private static final String lockssSolrSchemaVersionFieldName = "solrSchemaLockssVersion";

    /**
     * Reads the default value of the "solrSchemaLockssVersion" field from the core's schema (see managed-schema or
     * schema.xml file) for the version of the LOCKSS configuration set installed in the core.
     * <p>
     * Depreciated. This is an "alpha1" technique. The LOCKSS configuration set version is now recorded in the
     * configuration overlay as a userProps property.
     *
     * @param solrHome
     * @param coreName
     * @return
     * @throws IOException
     */
    @Deprecated
    public static int getLockssConfigSetVersionFromField(Path solrHome, String coreName) {
      // Get a CoreContainer contain cores from the Solr home base path
      CoreContainer cc = CoreContainer.createAndLoad(solrHome);

      try (SolrCore core = cc.getCore(coreName)) {
        if (core == null) {
          // Cannot determine config set version (core not found)
          return -1;
        }

        // Get index schema
        IndexSchema schema = core.getLatestSchema();

        if (schema.hasExplicitField(lockssSolrSchemaVersionFieldName)) {
          // Yes: Return default value of field
          return Integer.valueOf(schema.getField(lockssSolrSchemaVersionFieldName).getDefaultValue());
        } else {
          // No: Last resort: Check whether the schema is at version 1 based on the fields present
          if (hasSolrField(schema, "collection", "string") &&
              hasSolrField(schema, "auid", "string") &&
              hasSolrField(schema, "uri", "string") &&
              hasSolrField(schema, "committed", "boolean") &&
              hasSolrField(schema, "storageUrl", "string") &&
              hasSolrField(schema, "contentLength", "plong") &&
              hasSolrField(schema, "contentDigest", "string") &&
              hasSolrField(schema, "version", "pint")) {
//              hasSolrField(schema, "collectionDate", "long")) {
            // Yes: The schema is at version 1.
            return 1;
          }
        }
      } finally {
        // Shutdown the CoreContainer
        cc.shutdown();
      }

      // Could not determine LOCKSS configuration set version
      return -1;
    }

    /**
     * Returns a {@code boolean} indicating whether a field by the given field name and type exists in a
     * {@link IndexSchema}.
     *
     * @param schema       The {@link IndexSchema} to examine for field existence.
     * @param fieldName    A {@link String} containing the name of the field to check.
     * @param expectedType A {@link String} containing the expected field type.
     * @return A {@code boolean} indicating whether a field by the given field name and type exist in this
     * {@link IndexSchema}.
     */
    public static boolean hasSolrField(IndexSchema schema, String fieldName, String expectedType) {
      FieldType actualFieldType = schema.getField(fieldName).getType();
      return actualFieldType.getTypeName().equalsIgnoreCase(expectedType);
    }

    /**
     * Determines the version of a LOCKSS configuration set installed in this Solr core by looking up the value of the
     * "lockss-configset-version" key in user properties of its Solr configuration.
     * <p>
     * Returns 0 if the configuration overlay file could not be found, or the LOCKSS configuration set version key does
     * not exist in the overlay.
     *
     * @return An {@code int} containing the version of the configuration set.
     * @throws IOException
     */
    public static int getLockssConfigSetVersionFromSolrCore(SolrCore core) {
      Map<String, Object> userProps = core.getSolrConfig().getOverlay().getUserProps();

      log.trace("userProps = {}", userProps);

      return userProps.containsKey(LOCKSS_CONFIGSET_VERSION_KEY)
          ? Integer.parseInt(String.valueOf(userProps.get(LOCKSS_CONFIGSET_VERSION_KEY))) : 0;
    }

    /**
     * Retires the core's current configuration set by renaming it with a suffix containing a timestamp.
     *
     * @throws IOException
     */
    public void retireConfigSet() throws IOException {
      retirePath(configDirPath);
    }

    /**
     * Moves an existing directory or file out of the way by renaming it with a suffix containing a timestamp.
     *
     * @param targetPath
     * @throws IOException
     */
    public static void retirePath(Path targetPath) throws IOException {
      String timestamp = DateTimeFormatter.BASIC_ISO_DATE.format(LocalDateTime.now());
      String suffix = String.format("saved.%s", timestamp);
      addSuffix(targetPath, suffix);
    }

    /**
     * Adds a suffix extension to a directory or file. Appends a monotonically increasing whole number, starting at 1,
     * if the destination already exists.
     *
     * @param targetPath A {@code Path} to the directory or file.
     * @param suffix     A {@code String} containing the suffix to add.
     * @throws IOException
     */
    public static void addSuffix(Path targetPath, String suffix) throws IOException {

      // Base file name and default target path
      String baseFileName = String.format("%s.%s", targetPath.getFileName(), suffix);
      Path dstTargetPath = targetPath.getParent().resolve(baseFileName);

      // Get next step
      int step = getLatestFileNameSuffix(dstTargetPath) + 1;

      if (step > 0) {
        // New target file name and path containing step suffix
        String targetFileName = String.format("%s.%d", baseFileName, step);
        dstTargetPath = targetPath.getParent().resolve(targetFileName);
      }

      FileUtils.moveDirectory(targetPath.toFile(), dstTargetPath.toFile());
    }

    /**
     * Checks for siblings of the target path having the same name but numeric suffix and returns the maximum suffix. If
     * no such siblings exist but the target exists, it returns 0. Otherwise, returns -1 to indicate the target does not
     * exist.
     *
     * @param targetPath A {@code Path} containing the path to evaluate for maximum sibling suffix.
     * @return An {@code int} indicating the maximum sibling suffix, or 0 indicating the path has no stepped siblings,
     * or -1 if the target path does not exist.
     */
    public synchronized static int getLatestFileNameSuffix(Path targetPath) {
      String baseFileName = String.format("%s.", targetPath.getFileName());
      String[] allSiblingNames = targetPath.getParent().toFile().list();

      if (allSiblingNames != null) {
        OptionalInt optMaxStep = Arrays.stream(allSiblingNames)
            .filter(name -> name.startsWith(baseFileName))
            .map(name -> name.substring(baseFileName.length()))
            .filter(StringUtils::isNumeric)
            .mapToInt(Integer::parseInt)
            .max();

        if (optMaxStep.isPresent()) {
          return optMaxStep.getAsInt();
        }
      }

      // Return 0 to indicate the target exists but it has no stepped siblings; -1 to indicate the target does not exist
      return targetPath.toFile().exists() ? 0 : -1;
    }

    /**
     * Installs the specified version of the LOCKSS configuration set into this Solr core by reading the configuration
     * set's file list resource and copying the resources named within it to disk.
     *
     * @param version
     * @throws IOException
     */
    public void installLockssConfigSetVersion(int version) throws IOException {
      log.trace("Installing LOCKSS configuration set version {} to {}", version, configDirPath.toAbsolutePath());

      // Name of file list resource for this version
      String fileListResource = String.format("/solr/configsets/lockss/v%d/filelist.txt", version);

      // Read file list
      InputStream input = getClass().getResourceAsStream(fileListResource);

      try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
        // Name of resource to load
        String resourceName;

        // Iterate resources from list and copy each into the Core's configuration set path
        while ((resourceName = reader.readLine()) != null) {
          // Source resource URL
          URL srcUrl = getClass().getResource(
              String.format("/solr/configsets/lockss/v%d/conf/%s", version, resourceName)
          );

          // Destination file
          File dstFile = configDirPath.resolve(resourceName).toFile();

          // Copy resource to file
          FileUtils.copyURLToFile(srcUrl, dstFile);
        }
      }
    }

    /**
     * Returns a boolean indicating whether this Solr core uses a common configuration set.
     *
     * @return A {@code boolean} indicating whether this Solr core uses a common configuration set.
     */
    public boolean coreUsesCommonConfigSet() {
      return sharedConfigSetName != null;
    }

    /**
     * Returns a {@code LocalCoreUpdater} for a Solr core under a Solr home directory.
     * <p>
     * Returns {@code null} if the Solr core could not be found under the provided path.
     *
     * @param solrHome A {@code Path} to a Solr home directory.
     * @param coreName A {@code String} containing the name of the Solr core.
     * @return A {@code LocalCoreUpdater} instance or {@code null} if the core could not be found.
     */
    public static LocalSolrCoreAdmin fromSolrHomeAndCoreName(Path solrHome, String coreName) throws IOException {
      log.trace("solrHome = {}", solrHome.toAbsolutePath());
      log.trace("coreName = {}", coreName);

      CoreContainer container = CoreContainer.createAndLoad(solrHome);

      try {
        Map<String, CoreContainer.CoreLoadFailure> failureMap = container.getCoreInitFailures();

        if (failureMap.containsKey(coreName)) {
          throw new IOException("Failed to load core [coreName: " + coreName + "]", failureMap.get(coreName).exception);
        }

        try (SolrCore core = container.getCore(coreName)) {
          if (core != null) {
            return LocalSolrCoreAdmin.fromSolrCore(core);
          }
        }
      } finally {
        container.shutdown();
      }

      return null;
    }

    /**
     * Takes an an open {@code SolrCore} and extracts the configuration parameters to instantiate a new
     * {@code LocalSolrCoreAdmin} instance.
     *
     * @param core An instance of {@code SolrCore}.
     * @return An instance of {@code LocalSolrCoreAdmin} wrapping the given {@code SolrCore}.
     */
    public static LocalSolrCoreAdmin fromSolrCore(SolrCore core) {
      if (core == null) {
        throw new IllegalArgumentException("Null Solr core");
      }

      log.trace("getConfigName() = {}", core.getCoreDescriptor().getConfigName());
      log.trace("getConfigSet() = {}", core.getCoreDescriptor().getConfigSet());

      return new LocalSolrCoreAdmin(
          core.getName(),
          Paths.get(core.getCoreContainer().getSolrHome()),
          core.getResourceLoader().getInstancePath(),
          Paths.get(core.getResourceLoader().getConfigDir()),
          Paths.get(core.getDataDir()),
          Paths.get(core.getIndexDir()),
          core.getCoreDescriptor().getConfigSet(),

          // Determine config set directory path: Set to configuration set directory path under shared configuration set
          // base directory (as specified in solr.xml) if the core is configured to use a shared configuration set.
          core.getCoreDescriptor().getConfigSet() == null ?
              core.getCoreContainer().getNodeConfig().getConfigSetBaseDirectory() :
              SolrXmlConfig.fromSolrHome(Paths.get(core.getCoreContainer().getSolrHome()), null)
                  .getConfigSetBaseDirectory()
                  .resolve(core.getCoreDescriptor().getConfigSet()),

          getLockssConfigSetVersionFromSolrCore(core)
      );
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      LocalSolrCoreAdmin coreAdmin = (LocalSolrCoreAdmin) o;
      return lockssConfigSetVersion == coreAdmin.lockssConfigSetVersion &&
          solrCoreName.equals(coreAdmin.solrCoreName) &&
          solrHome.equals(coreAdmin.solrHome) &&
          instanceDir.equals(coreAdmin.instanceDir) &&
          configDirPath.equals(coreAdmin.configDirPath) &&
          dataDir.equals(coreAdmin.dataDir) &&
          indexDir.equals(coreAdmin.indexDir) &&
          Objects.equals(sharedConfigSetName, coreAdmin.sharedConfigSetName) &&
          Objects.equals(sharedConfigSetBaseDir, coreAdmin.sharedConfigSetBaseDir);
    }

    @Override
    public int hashCode() {
      return Objects.hash(solrCoreName, solrHome, instanceDir, configDirPath, dataDir, indexDir, sharedConfigSetName, sharedConfigSetBaseDir, lockssConfigSetVersion);
    }

    public String getCoreName() {
      return this.solrCoreName;
    }

    public void reindexLatest() throws IOException, SolrResponseErrorException, SolrServerException {
      try (EmbeddedSolrServer solrClient = new EmbeddedSolrServer(solrHome, solrCoreName)) {

        File reindexLockFile = indexDir.resolve(REINDEX_LOCK_FILE).toFile();

        if (reindexLockFile.createNewFile()) {

          // Acquired reindex lock: Perform reindex
          SolrArtifactIndexReindex.reindexArtifactsForVersion(solrClient, null, getCoreName(),
              LATEST_LOCKSS_CONFIGSET_VERSION);
          FileUtils.forceDelete(reindexLockFile);

        } else {

          // Could not acquire reindex lock (reindex lock file already exists)
          log.trace("Reindex is already in progress");

        }
      }
    }
  }

  // SOLR CLOUD ////////////////////////////////////////////////////////////////////////////////////////////////////////

  public static class SolrCloudCollectionAdmin {
    CloudSolrClient cloudClient;
    List<String> solrCredentials;

    public SolrCloudCollectionAdmin(CloudSolrClient cloudClient, List<String> solrCredentials) {
      this.cloudClient = cloudClient;
      this.solrCredentials = solrCredentials;
    }

    public static SolrCloudCollectionAdmin fromCloudSolrClient(
        CloudSolrClient cloudClient, List<String> solrCredentials) {
      return new SolrCloudCollectionAdmin(cloudClient, solrCredentials);
    }

    /**
     * Creates a CloudSolrClient instance given Solr and ZooKeeper endpoints.
     *
     * @param solrUrl A {@code String} containing the URL to a Solr REST endpoint.
     * @param zkHost  A {@code String} containing the host and port of the ZooKeeper instance used by the Solr Cloud.
     * @return A {@code CloudSolrClient} instance.
     */
    public static CloudSolrClient createSolrCloudClient(String solrUrl, String zkHost) {
      CloudSolrClient.Builder builder = new CloudSolrClient.Builder();
      builder.withSolrUrl(solrUrl);
      builder.withZkHost(zkHost);

      return builder.build();
    }

    public void update() {
      try {
//        updateLuceneIndex();
        uploadConfigSet(cloudClient.getDefaultCollection());
//        applySchemaUpdates();
      } catch (IOException | SolrResponseErrorException | SolrServerException e) {
        log.error("Caught exception while attempting to upgrade Solr Cloud collection [collection: "
            + cloudClient.getDefaultCollection() + "]"
        );
      }
    }

    /*
    public static int getLockssConfigSetVersion() {
      Map<String, Object> userProps = core.getSolrConfig().getOverlay().getUserProps();

      log.trace("userProps = {}", userProps);

      return userProps.containsKey(userProps)
          ? Integer.parseInt(String.valueOf(userProps.get(LOCKSS_CONFIGSET_VERSION_KEY))) : 0;
    }
    */

    public static int getLockssConfigSetVersion() {
      // TODO
      return 0;
    }

    public void uploadConfigSet(String collection) throws IOException, SolrResponseErrorException, SolrServerException {
      int lockssConfigSetVersion = getLockssConfigSetVersion();
      int targetVersion = LATEST_LOCKSS_CONFIGSET_VERSION;

      log.trace("lockssConfigSetVersion = {}", lockssConfigSetVersion);
      log.trace("targetVersion = {}", targetVersion);

      // Update index to target version iteratively
      for (int version = lockssConfigSetVersion; version < targetVersion; version++) {
        // Install configuration set
        installConfigSetVersion(version + 1);

        // Perform post-installation tasks
        SolrArtifactIndexReindex.reindexArtifactsForVersion(cloudClient, solrCredentials, collection, version + 1);

        lockssConfigSetVersion = version + 1;
      }
    }

    private void installConfigSetVersion(int i) throws IOException {
      try (SolrZkClient zkClient = new SolrZkClient(cloudClient.getZkHost(), 10)) {
        // Use SolrZkClient to upload config set
        zkClient.upConfig(Paths.get(SOLR_CONFIGSET_PATH), SOLR_CONFIGSET_NAME);

        // Use ZkConfigManager to upload config set
//        new ZkConfigManager(zkClient).uploadConfigDir(Paths.get(SOLR_CONFIGSET_PATH), SOLR_CONFIGSET_NAME);
      }
    }

    public void applySchemaUpdates(String collection) {
      try {
        new ArtifactIndexSchemaUpdater(cloudClient, solrCredentials).updateSchema(collection,
            LATEST_LOCKSS_CONFIGSET_VERSION);
      } catch (SolrResponseErrorException | SolrServerException | IOException e) {
        e.printStackTrace();
      }
    }

    public static final String SOLR_CONFIGSET_PATH = "target/test-classes/solr";
    public static final String SOLR_CONFIGSET_NAME = "testConfig";

    /**
     * Returns a boolean indicating whether a collection exists in a Solr Cloud cluster.
     *
     * @param collectionName
     * @return
     * @throws IOException
     * @throws SolrServerException
     */
    public boolean collectionExists(String collectionName) throws IOException, SolrServerException {
      List<String> collections = CollectionAdminRequest.listCollections(cloudClient);
      return collections.contains(collectionName);
    }
  }

  // ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  // Constants for command-line options
  public static final String KEY_ENDPOINT = "endpoint";
  public static final String KEY_SOLRUSERNAME = "user";
  public static final String KEY_SOLRPASSWORD = "password";
  public static final String KEY_ACTION = "action";
  public static final String KEY_CORE = "core";
  public static final String KEY_LOCAL = "local";
  public static final String KEY_COLLECTION = "collection";
  public static final String KEY_CLOUD = "cloud";
  public static final String KEY_ZKHOST = "zkHost";

  public static final String KEY_LOCALDS = "localds";

  private enum AdminExitCode {
    OK(0),
    MISSING_CORE(1),
    UPDATE_NEEDED(2),
    UPDATE_INPROGRESS(4),
    UPDATE_FAILURE(8),
    ERROR(Integer.MAX_VALUE);

    int status;

    AdminExitCode(int status) {
      this.status = status;
    }

    public int getStatus() {
      return status;
    }
  }

  private static void exit(AdminExitCode exitCode) {
    System.exit(exitCode.getStatus());
  }

  public static void runAction(CommandLine cmd) throws IOException, SolrServerException, SolrResponseErrorException {
    if (cmd.hasOption(KEY_LOCAL) && cmd.hasOption(KEY_CLOUD)) {
      throw new IllegalArgumentException("Both --local and --cloud may not be specified at the same time");
    }

    // Determine name of Solr core to update (or use the default)
    String coreName = cmd.hasOption(KEY_CORE) ?
        cmd.getOptionValue(KEY_CORE) : DEFAULT_SOLRCORE_NAME;

    if (cmd.hasOption(KEY_LOCAL)) {

      // Path to Solr home
      Path solrHome = Paths.get(cmd.getOptionValue(KEY_LOCAL));

      switch (cmd.getOptionValue(KEY_ACTION)) {
        case "create":
          try {
            LocalSolrCoreAdmin.createCore(Paths.get(cmd.getOptionValue(KEY_LOCAL)), coreName);
          } catch (SolrException e) {
            if (e.getMessage().contains("already exists")) {
              log.info("Core already exists");
              break;
            }

            // Re-throw unknown SolrException
            throw e;
          }
          break;

        case "update":
          // Update the local Solr core
          LocalSolrCoreAdmin admin1 = LocalSolrCoreAdmin.fromSolrHomeAndCoreName(solrHome, coreName);
          admin1.update();
          break;

        case "verify":
          /*
          Exit codes:
          0 = Solr core is up-to-date
          1 = Solr core is missing
          2 = Solr core needs an update (Lucene index or LOCKSS configuration set)
          4 = Solr core update and reindex in-progress (normal)
          8 = Solr core update not in-progress but reindex file present (interrupted)
          */
          LocalSolrCoreAdmin admin2 = LocalSolrCoreAdmin.fromSolrHomeAndCoreName(solrHome, coreName);

          if (admin2 != null) {
            boolean updateInProgress = admin2.isUpdateInProgress();
            boolean reindexInProgress = admin2.isReindexInProgress();

            if (!updateInProgress && reindexInProgress) {
              log.error("Detected interrupted update!");
              exit(AdminExitCode.UPDATE_FAILURE);
            }

            if (updateInProgress && reindexInProgress) {
              log.info("Update in-progress in another thread or JVM");
              exit(AdminExitCode.UPDATE_INPROGRESS);
            }

            // Core exists: Verify successful upgrade should fail if the core has an update available, or if the core is
            //              in the middle of a reindex.
            if (admin2.isUpdateAvailable()) {
              log.info("Update available");
              exit(AdminExitCode.UPDATE_NEEDED);
            }

            // Core is up-to-date
            System.exit(0);
          } else {
            // Core not found
            log.error("Core not found!");
            exit(AdminExitCode.MISSING_CORE);
          }

          break;

        case "force-reindex":
          try {
            // Force a reindex
            LocalSolrCoreAdmin admin3 = LocalSolrCoreAdmin.fromSolrHomeAndCoreName(solrHome, coreName);
            admin3.reindexLatest();
          } catch (Exception e) {
            log.error("Caught exception while forcing a full reindex to latest artifact schema", e);
          }
          break;

        case "rebuild":
          if (!cmd.hasOption(KEY_LOCALDS)) {
            log.error("No local data store base directories specified");
            exit(AdminExitCode.ERROR);
          }

          // Parse semcolon delimited list of local artifact data store base directories
          String[] dirs = cmd.getOptionValue(KEY_LOCALDS).split(";");
          File[] baseDirs = Arrays.stream(dirs).map(File::new).toArray(File[]::new);

          // Rebuild local Solr artifact index from local data store
          try (EmbeddedSolrServer solrClient = new EmbeddedSolrServer(solrHome, coreName)) {
            SolrArtifactIndex index = new SolrArtifactIndex(solrClient, coreName);
            LocalWarcArtifactDataStore ds = new LocalWarcArtifactDataStore(baseDirs);
            ds.reindexArtifacts(index);
          }

          break;

        case "upgrade-lucene-index":
          LocalSolrCoreAdmin admin4 = LocalSolrCoreAdmin.fromSolrHomeAndCoreName(solrHome, coreName);
          admin4.upgradeLuceneIndex();
          System.exit(0);
          break;

        case "update-lockss-configset":
          /*
          Exit codes:
          0 = Installed new LOCKSS configuration set successfully
          1 = Core already has latest LOCKSS configuration set
          2 = An error occurred while installing new LOCKSS configuration set
          */
          try {
            LocalSolrCoreAdmin admin5 = LocalSolrCoreAdmin.fromSolrHomeAndCoreName(solrHome, coreName);

            if (admin5.isLockssConfigSetUpdateAvailable()) {
              // Target version to install
              int targetVersion = admin5.getLockssConfigSetVersion() + 1;

              // Install next LOCKSS configuration set version
              admin5.retireConfigSet();
              admin5.installLockssConfigSetVersion(targetVersion);
              System.exit(0);
            } else {
              // Core has latest LOCKSS configuration set
              System.exit(1);
            }
          } catch (Exception e) {
            log.error("Could not install LOCKSS configuration set", e);
            System.exit(2);
          }
          break;

        default:
          log.error("Unknown action to perform [action: {}]", cmd.getOptionValue(KEY_ACTION));
          throw new IllegalArgumentException("Unknown action");
      }

    } else if (cmd.hasOption(KEY_ENDPOINT)) {

      // Get SolrClient from Solr REST endpoint
      HttpSolrClient solrClient = new HttpSolrClient.Builder()
          .withBaseSolrUrl(cmd.getOptionValue(KEY_ENDPOINT))
          .build();

      try {
        switch (cmd.getOptionValue(KEY_ACTION)) {
          case "apply-lockss-configset-changes":

            // Determine target version
            CoreConfigRequest req = new CoreConfigRequest.Overlay();

            // Set Solr BasicAuth credentials if present
            if (cmd.hasOption(KEY_SOLRUSERNAME)) {
              req.setBasicAuthCredentials(
                  cmd.getOptionValue(KEY_SOLRUSERNAME),
                  cmd.getOptionValue(KEY_SOLRPASSWORD));
            }

            CoreConfigResponse.Overlay res = (CoreConfigResponse.Overlay) req.process(solrClient);
            int targetVersion = res.getLockssConfigSetVersion();

            List<String> credentials = null;

            if (cmd.hasOption(KEY_SOLRUSERNAME)) {
              credentials = ListUtil.list(
                  cmd.getOptionValue(KEY_SOLRUSERNAME),
                  cmd.getOptionValue(KEY_SOLRPASSWORD)
              );
            }

            // Apply changes for target LOCKSS configuration set version
            //TODO give this method a new name
            SolrArtifactIndexReindex.reindexArtifactsForVersion(solrClient, credentials, coreName, targetVersion);

            break;

          case "rebuild":
            if (!cmd.hasOption(KEY_LOCALDS)) {
              log.error("No local data store base directories specified");
              exit(AdminExitCode.ERROR);
            }

            // Parse semi-colon delimited list of local artifact data store base directories
            String[] dirs = cmd.getOptionValue(KEY_LOCALDS).split(";");
            File[] baseDirs = Arrays.stream(dirs).map(File::new).toArray(File[]::new);

            // Rebuild the Solr index from the local data store
            SolrArtifactIndex index = new SolrArtifactIndex(solrClient, coreName);
            LocalWarcArtifactDataStore ds = new LocalWarcArtifactDataStore(baseDirs);
            ds.reindexArtifacts(index);

            break;

          default:
            throw new IllegalArgumentException("Unknown action: " + cmd.getOptionValue(KEY_ACTION));
        }
      } finally {
        solrClient.close();
      }


    } else if (cmd.hasOption(KEY_CLOUD)) {

      // Get Solr Cloud endpoint from command-line argument
      String solrUrl = cmd.getOptionValue(KEY_CLOUD);

      try (CloudSolrClient cloudClient = new CloudSolrClient.Builder()
          .withSolrUrl(solrUrl)
          .withZkHost(cmd.getOptionValue(KEY_ZKHOST))
          .build()) {

        // Determine name of Solr collection to update (or use the default)
        String collection = cmd.hasOption(KEY_COLLECTION) ?
            cmd.getOptionValue(KEY_COLLECTION) : DEFAULT_SOLRCORE_NAME;

        // Set default Solr Cloud client collection
        cloudClient.setDefaultCollection(collection);

        List<String> credentials = null;

        if (cmd.hasOption(KEY_SOLRUSERNAME)) {
          credentials = ListUtil.list(
              cmd.getOptionValue(KEY_SOLRUSERNAME),
              cmd.getOptionValue(KEY_SOLRPASSWORD)
          );
        }

        SolrCloudCollectionAdmin updater = SolrCloudCollectionAdmin.fromCloudSolrClient(cloudClient, credentials);
        updater.update();
      }

    } else {
      throw new IllegalArgumentException("--local, --endpoint, or --cloud must be specified");
    }
  }

  public static void main(String[] args) throws ParseException, IOException, SolrResponseErrorException, SolrServerException {

    // Define command-line options
    Options options = new Options();

    options.addOption(null, KEY_ACTION, true, "Action to perform (create, update, force-reindex, rebuild or verify)");

    // Local
    options.addOption(null, KEY_CORE, true, "Name of Solr core");
    options.addOption(null, KEY_LOCAL, true, "Path to Solr home base directory");

    // Endpoint
    options.addOption(null, KEY_ENDPOINT, true, "Solr REST endpoint");
    options.addOption(null, KEY_SOLRUSERNAME, true, "Solr username");
    options.addOption(null, KEY_SOLRPASSWORD, true, "Solr password");

    // Solr Cloud
    options.addOption(null, KEY_COLLECTION, true, "Name of Solr Cloud collection");
    options.addOption(null, KEY_CLOUD, true, "Solr Cloud REST endpoint");
    options.addOption(null, KEY_ZKHOST, true, "ZooKeeper REST endpoint used by Solr Cloud cluster");

    // Data store source (for index rebuilds)
    options.addOption(null, KEY_LOCALDS, true, "Local data store base directories");

    try {
      // Parse command-line options and execute action
      CommandLine cmd = new PosixParser().parse(options, args);
      runAction(cmd);
      exit(AdminExitCode.OK);
    } catch (Exception e) {
      log.error("Exception caught", e);
      exit(AdminExitCode.ERROR);
    }
  }

}
