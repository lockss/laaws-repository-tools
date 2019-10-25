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

import org.apache.commons.cli.*;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.schema.FieldTypeDefinition;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.*;
import org.json.JSONObject;
import org.lockss.laaws.rs.io.index.solr.SolrArtifactIndex;
import org.lockss.laaws.rs.io.index.solr.SolrQueryArtifactIterator;
import org.lockss.laaws.rs.io.index.solr.SolrResponseErrorException;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.log.L4JLogger;
import org.lockss.util.io.FileUtil;

import java.io.*;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

public class SolrArtifactIndexAdmin {
  private final static L4JLogger log = L4JLogger.getLogger();

  /**
   * Latest version of the LOCKSS configuration set for {@link SolrArtifactIndex}.
   */
  public static final int LATEST_LOCKSS_CONFIGSET_VERSION = 3;

  /**
   * Target version of the Lucene index and segments.
   */
  public static final Version TARGET_LUCENE_VERSION = Version.fromBits(7, 2, 1);

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
    public static void reindexArtifactsForVersion(SolrClient solrClient, int targetVersion)
        throws SolrServerException, SolrResponseErrorException, IOException {

      log.trace("targetVersion = {}", targetVersion);

      try {

        switch (targetVersion) {
          case 2:
            reindexArtifactsFrom1To2(solrClient);
            break;

          case 3:
            reindexArtifactsFrom2To3(solrClient);
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
    public static void reindexArtifactsFrom1To2(SolrClient solrClient) throws IOException, SolrServerException, SolrResponseErrorException {
      try {

        // Loop through all the documents in the index.
        SolrQuery q = new SolrQuery().setQuery("*:*");

        for (Artifact artifact : IteratorUtils.asIterable(new SolrQueryArtifactIterator(solrClient, q))) {
          // Create a Solr input document
          SolrInputDocument document = new SolrInputDocument();

          // Specify the ID of the Solr document (artifact) to modify
          document.addField("id", artifact.getId());

          // This is for the new sortUri field
          document.addField("sortUri", getFieldModifier("set", artifact.getSortUri()));

          // This is for long to plong conversion
          document.addField("collectionDate", getFieldModifier("set", artifact.getCollectionDate()));

          log.trace("document = {}", document);

          // Add the document with the new field.
          handleSolrResponse(solrClient.add(document), "Problem adding artifact '" + document + "' to Solr");
        }

        // Commit all changes
        handleSolrResponse(solrClient.commit(), "Problem committing changes to Solr");

      } catch (IOException | SolrServerException | SolrResponseErrorException e) {

        String errorMessage = "Caught exception while performing post configset installation tasks for version 2";
        log.error(errorMessage, e);
        throw e;

      }
    }

    private static void reindexArtifactsFrom2To3(SolrClient solrClient) throws IOException, SolrServerException, SolrResponseErrorException {
      reindexAllArtifacts(solrClient);
    }

    /**
     * Experimental. Performs an in-place reindex of all artifacts in the Solr index. All fields must be "stored" in the
     * index's schema for this to work.
     */
    public static void reindexAllArtifacts(SolrClient solrClient)
        throws SolrServerException, SolrResponseErrorException, IOException {

      try {
        // Match all documents
        SolrQuery q = new SolrQuery().setQuery("*:*");

        // Loop through all the documents in the index.
        for (Artifact artifact : IteratorUtils.asIterable(new SolrQueryArtifactIterator(solrClient, q))) {
          handleSolrResponse(
              solrClient.addBean(artifact),
              "Problem reindexing artifact [artifactId: " + artifact.getId() + "]"
          );
        }

        // Commit all changes
        handleSolrResponse(solrClient.commit(), "Problem committing changes to Solr");

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

    public ArtifactIndexSchemaUpdater(SolrClient client) {
      this.solrClient = client;
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
    private void updateSchema(int targetSchemaVersion)
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
        int lastUpdatedVersion = updateSchema(existingSchemaVersion, targetSchemaVersion);
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
    private int updateSchema(int existingVersion, int finalVersion)
        throws SolrResponseErrorException, SolrServerException, IOException {

      log.debug2("existingVersion = {}", existingVersion);
      log.debug2("finalVersion = {}", finalVersion);

      int lastUpdatedVersion = existingVersion;

      // Loop through all the versions to be updated to reach the targeted
      // version.
      for (int from = existingVersion; from < finalVersion; from++) {
        log.trace("Updating from version {}...", from);

        // Perform the appropriate update of the Solr schema for this version.
        updateSchemaToVersion(from + 1);

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
    private void updateSchemaToVersion(int targetVersion)
        throws SolrResponseErrorException, SolrServerException, IOException {

      log.debug2("targetVersion = {}", targetVersion);

      // Perform the appropriate Solr schema update for this version.
      try {
        switch (targetVersion) {
          case 1:
            updateSchemaFrom0To1();
            break;
          case 2:
            updateSchemaFrom1To2();
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
    private void updateSchemaFrom1To2()
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

        for (Artifact artifact : IteratorUtils.asIterable(new SolrQueryArtifactIterator(solrClient, q))) {
          // Initialize a document with the artifact identifier.
          SolrInputDocument document = new SolrInputDocument();
          document.addField("id", artifact.getId());

          // Add the new field value.
          document.addField("sortUri", getFieldModifier("set", artifact.getSortUri()));
          document.addField("collectionDate", getFieldModifier("set", artifact.getCollectionDate()));
          log.trace("document = {}", document);

          // Add the document with the new field.
          handleSolrResponse(solrClient.add(document), "Problem adding document '" + document + "' to Solr");
        }

        // Commit all changes
        handleSolrResponse(solrClient.commit(), "Problem committing changes to Solr");

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
    public void update() {
      try {
        updateLuceneIndex();
        updateConfigSet();
//      applySchemaUpdates();
      } catch (IOException | SolrResponseErrorException | SolrServerException e) {
        log.error("Caught exception while attempting to upgrade Solr core [core: " + solrCoreName + "]");
        // TODO better error handling
      }
    }

    /**
     * Applies schema updates to this Solr core using the Schema API through SolrJ.
     */
    @Deprecated
    public void applySchemaUpdates() {
      log.debug("Applying schema updates through SolrJ");

      try (SolrClient solrClient = new EmbeddedSolrServer(solrHome, solrCoreName)) {
        new ArtifactIndexSchemaUpdater(solrClient).updateSchema(LATEST_LOCKSS_CONFIGSET_VERSION);
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
    public void updateLuceneIndex() throws IOException {
      // Get the minimum version of all the committed segments in this Lucene index
      SegmentInfos segInfos = getSegmentInfos();

      log.trace("segInfos.size() = {}", segInfos.size());

      // Index must have at least one segment
      if (segInfos.size() > 0) {
        // Get minimum version among segments
        Version minSegVersion = segInfos.getMinSegmentLuceneVersion();

        log.trace("minSegVersion = {}", minSegVersion);

        // Run the IndexUpgrader tool if the minimum version is not on or after the latest version
        if (!minSegVersion.onOrAfter(TARGET_LUCENE_VERSION)) {
          log.trace("Running IndexUpgrader [minVersion: {}, latestVersion: {}]", minSegVersion, Version.LATEST);
          new IndexUpgrader(FSDirectory.open(indexDir)).upgrade();
        } else {
          log.trace("IndexUpgrader not necessary [minVersion: {}, latestVersion: {}]", minSegVersion, Version.LATEST);
        }
      } else {
        log.debug2("No segments to upgrade");
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

      int targetVersion = LATEST_LOCKSS_CONFIGSET_VERSION;

      if (lockssConfigSetVersion <= 0) {
        // Yes: Invalid configuration set version: Attempt to ready from configuration overlay file.
        lockssConfigSetVersion = getLockssConfigSetVersion();
      }

      log.trace("currentVersion = {}", lockssConfigSetVersion);
      log.trace("targetVersion = {}", targetVersion);

      if (lockssConfigSetVersion == targetVersion) {
        log.trace("Already at latest; nothing to do");
        return;
      }

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
          SolrArtifactIndexReindex.reindexArtifactsForVersion(solrClient, version + 1);
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

    /**
     * Determines the version of a LOCKSS Solr configuration set by reading its configuration overlay JSON file directly
     * and returning the value of the "lockss-configset-version" key.
     * <p>
     * Returns 0 if the configuration overlay file could not be found, or if the LOCKSS configuration set version key
     * does not exist in the overlay.
     *
     * @return An {@code int} containing the version of the configuration set.
     * @throws IOException
     */
    public int getLockssConfigSetVersion() throws IOException {
      return getLockssConfigSetVersion(configDirPath) ;
    }

    public static int getLockssConfigSetVersion(Path configDir) throws IOException {
      if (Objects.isNull(configDir)) {
        throw new IllegalArgumentException("Null configuration set path");
      }

      File configOverlayPath = configDir.resolve(CONFIGOVERLAY_FILE).toFile();

      if (configOverlayPath.exists() && configOverlayPath.isFile()) {
        try (InputStream input = new FileInputStream(configOverlayPath)) {
          JSONObject json = new JSONObject(IOUtils.toString(input, "UTF-8"));

          if (json.has(CONFIGOVERLAY_USERPROPS_KEY)) {
            JSONObject userProps = json.getJSONObject(CONFIGOVERLAY_USERPROPS_KEY);

            if (userProps.has(LOCKSS_CONFIGSET_VERSION_KEY)) {
              return userProps.getInt(LOCKSS_CONFIGSET_VERSION_KEY);
            }
          }
        }
      }

      return 0;
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
    public static int getLockssConfigSetVersion(SolrCore core) {
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

      OptionalInt optMaxStep = Arrays.stream(allSiblingNames)
          .filter(name -> name.startsWith(baseFileName))
          .map(name -> name.substring(baseFileName.length()))
          .filter(StringUtils::isNumeric)
          .mapToInt(Integer::parseInt)
          .max();

      if (optMaxStep.isPresent()) {
        return optMaxStep.getAsInt();
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
    public static LocalSolrCoreAdmin fromSolrHomeAndCoreName(Path solrHome, String coreName) {
      LocalSolrCoreAdmin updater = null;

      log.trace("solrHome = {}", solrHome.toAbsolutePath());

      CoreContainer container = CoreContainer.createAndLoad(solrHome);

      Map<String, SolrCore> cores = container.getCores().stream().collect(
          Collectors.toMap(
              core -> core.getName(),
              core -> core
          )
      );

      log.trace("cores.keySet() = {}", cores.keySet());

      if (cores.containsKey(coreName)) {
        updater = LocalSolrCoreAdmin.fromSolrCore(cores.get(coreName));
      }

      container.shutdown();

      return updater;
    }

    /**
     * Takes an an open {@code SolrCore} and extracts the configuration parameters to instantiate a new
     * {@code LocalSolrCoreAdmin} instance.
     *
     * @param core An instance of {@code SolrCore}.
     * @return An instance of {@code LocalSolrCoreAdmin} wrapping the given {@code SolrCore}.
     */
    public static LocalSolrCoreAdmin fromSolrCore(SolrCore core) {
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
              SolrXmlConfig.fromSolrHome(Paths.get(core.getCoreContainer().getSolrHome()))
                  .getConfigSetBaseDirectory()
                  .resolve(core.getCoreDescriptor().getConfigSet()),

          getLockssConfigSetVersion(core)
      );
    }
  }

  // SOLR CLOUD ////////////////////////////////////////////////////////////////////////////////////////////////////////

  public static class SolrCloudCollectionAdmin {
    CloudSolrClient cloudClient;

    public SolrCloudCollectionAdmin(CloudSolrClient cloudClient) {
      this.cloudClient = cloudClient;
    }

    public static SolrCloudCollectionAdmin fromCloudSolrClient(CloudSolrClient cloudClient) {
      return new SolrCloudCollectionAdmin(cloudClient);
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
        uploadConfigSet();
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

    public void uploadConfigSet() throws IOException, SolrResponseErrorException, SolrServerException {
      int lockssConfigSetVersion = getLockssConfigSetVersion();
      int targetVersion = LATEST_LOCKSS_CONFIGSET_VERSION;

      log.trace("lockssConfigSetVersion = {}", lockssConfigSetVersion);
      log.trace("targetVersion = {}", targetVersion);

      // Update index to target version iteratively
      for (int version = lockssConfigSetVersion; version < targetVersion; version++) {
        // Install configuration set
        installConfigSetVersion(version + 1);

        // Perform post-installation tasks
        SolrArtifactIndexReindex.reindexArtifactsForVersion(cloudClient, version + 1);

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

    public void applySchemaUpdates() {
      try {
        new ArtifactIndexSchemaUpdater(cloudClient).updateSchema(LATEST_LOCKSS_CONFIGSET_VERSION);
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
  public static final String KEY_ACTION = "action";
  public static final String KEY_CORE = "core";
  public static final String KEY_LOCAL = "local";
  public static final String KEY_COLLECTION = "collection";
  public static final String KEY_CLOUD = "cloud";
  public static final String KEY_ZKHOST = "zkHost";

  public static void main(String[] args) throws ParseException, IOException, SolrResponseErrorException, SolrServerException {

    // Define command-line options
    Options options = new Options();

    options.addOption(null, KEY_ACTION, true, "Action to perform (create or upgrade)");

    // Local
    options.addOption(null, KEY_CORE, true, "Name of Solr core");
    options.addOption(null, KEY_LOCAL, true, "Path to Solr home base directory");

    // Solr Cloud
    options.addOption(null, KEY_COLLECTION, true, "Name of Solr Cloud collection");
    options.addOption(null, KEY_CLOUD, true, "Solr Cloud REST endpoint");
    options.addOption(null, KEY_ZKHOST, true, "ZooKeeper REST endpoint used by Solr Cloud cluster");

    // Parse command-line options
    CommandLine cmd = new PosixParser().parse(options, args);

    if (cmd.hasOption(KEY_LOCAL) && cmd.hasOption(KEY_CLOUD)) {
      throw new IllegalArgumentException("Both --local and --cloud may not be specified at the same time");
    }

    if (cmd.hasOption(KEY_LOCAL)) {

      // Path to Solr home
      Path solrHome = Paths.get(cmd.getOptionValue(KEY_LOCAL));

      // Determine name of Solr core to update (or use the default)
      String coreName = cmd.hasOption(KEY_CORE) ?
          cmd.getOptionValue(KEY_CORE) : SolrArtifactIndex.DEFAULT_SOLRCORE_NAME;

      switch (cmd.getOptionValue(KEY_ACTION)) {
        case "create":
          LocalSolrCoreAdmin.createCore(Paths.get(cmd.getOptionValue(KEY_LOCAL)), coreName);
          break;

        case "update":
          // Update the local Solr core
          LocalSolrCoreAdmin updater = LocalSolrCoreAdmin.fromSolrHomeAndCoreName(solrHome, coreName);
          updater.update();
          break;

        default:
          log.error("Unknown action to perform [action: {}]", cmd.getOptionValue(KEY_ACTION));
          throw new IllegalArgumentException("Unknown action");
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
            cmd.getOptionValue(KEY_COLLECTION) : SolrArtifactIndex.DEFAULT_SOLRCORE_NAME;

        // Set default Solr Cloud client collection
        cloudClient.setDefaultCollection(collection);

        SolrCloudCollectionAdmin updater = SolrCloudCollectionAdmin.fromCloudSolrClient(cloudClient);
        updater.update();
      }

      /*
      // Get a HttpSolrClient to the remote Solr endpoint
      try (SolrClient solrClient = new HttpSolrClient.Builder(solrUrl).build()) {
        // Get Solr server mode
        String mode = AdminInfoRequest.getSystemInfo(solrClient).getSolrMode();

        // Different upgrade paths depending on Solr server mode
        switch (mode) {
          case "std":
            // Do stuff related to Solr standalone mode
            updateRemoteSolrCore(solrClient);
            break;

          case "solrcloud":
            try (CloudSolrClient cloudClient = new CloudSolrClient.Builder()
                .withSolrUrl(solrUrl)
                .withZkHost(cmd.getOptionValue("zkHost"))
                .build()) {

              // Determine name of Solr collection to update (or use the default)
              String collection = cmd.hasOption("collection") ?
                  cmd.getOptionValue("collection") : SolrArtifactIndex.DEFAULT_SOLRCORE_NAME;

              // Do stuff related to Solr Cloud mode
              updateSolrCloud(cloudClient);
            }
            break;

          default:
            log.error("Remote Solr server running in unknown mode [solrUrl: {}, mode: {}]", solrUrl, mode);
            throw new IllegalStateException("Solr server running in unknown mode");
        }

      } catch (Exception e) {
        e.printStackTrace();
      }
      */

    } else {
      throw new IllegalArgumentException("Nothing to do; --local or --cloud must be specified");
    }
  }

}
