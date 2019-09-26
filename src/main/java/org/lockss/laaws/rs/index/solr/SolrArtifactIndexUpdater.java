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
import org.apache.lucene.index.*;
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
import org.apache.solr.client.solrj.request.CoreStatus;
import org.apache.solr.client.solrj.request.schema.FieldTypeDefinition;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.SolrResponseBase;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.*;
import org.json.JSONObject;
import org.lockss.laaws.rs.io.index.solr.SolrQueryArtifactIterator;
import org.lockss.laaws.rs.io.index.solr.SolrResponseErrorException;
import org.lockss.laaws.rs.model.Artifact;
import org.lockss.log.L4JLogger;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SolrArtifactIndexUpdater {
  private final static L4JLogger log = L4JLogger.getLogger();

  private static final int TARGET_SCHEMA_VERSION = 2;


  // COMMON ////////////////////////////////////////////////////////////////////////////////////////////////////////////

  // SCHEMA API ////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Returns a Solr response unchanged, if it has a zero status; throws,
   * otherwise.
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
      Map<String, Object> field =
          solrSchemaFields.get(lockssSolrSchemaVersionFieldName);

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
     * Additional field attributes can be provided, or default attributes can be overridden, by passing field attributes.
     *
     * @param solr            A {@code SolrClient} that points to a Solr core or collection to add the field to.
     * @param fieldName       A {@code String} containing the name of the new field.
     * @param fieldType       A {@code String} containing the name of the type of the new field.
     * @param fieldAttributes A {@code Map<String, Object>} containing additional field attributes, and/or a map of fields to override.
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

  public static class LocalCoreUpgrader {
    String solrCoreName;
    Path solrHome;
    Path instanceDir;
    Path configDir;
    Path dataDir;
    Path indexDir;
    String configSet;
    Path configSetBaseDir;

    public LocalCoreUpgrader(SolrCore core) {
      this(
          core.getName(),
          Paths.get(core.getCoreContainer().getSolrHome()),
          core.getResourceLoader().getInstancePath(),
          Paths.get(core.getResourceLoader().getConfigDir()),
          Paths.get(core.getDataDir()),
          Paths.get(core.getIndexDir()),
          core.getCoreDescriptor().getConfigSet(),
          core.getCoreContainer().getNodeConfig().getConfigSetBaseDirectory()
      );
    }

    public LocalCoreUpgrader(String name, Path solrHome, Path instanceDir, Path configDir, Path dataDir, Path indexDir, String configSet, Path configSetBaseDir) {
      this.solrCoreName = name;
      this.solrHome = solrHome;
      this.instanceDir = instanceDir;
      this.configDir = configDir;
      this.dataDir = dataDir;
      this.indexDir = indexDir;
      this.configSet = configSet;
      this.configSetBaseDir = configSetBaseDir;
    }

    /**
     * Performs an upgrade of the Solr core's Lucene index if necessary.
     *
     * @throws IOException
     */
    public void updateLuceneIndex() throws IOException {
      // Get the minimum version of all the committed segments in this Lucene index
      SegmentInfos segInfos = SegmentInfos.readLatestCommit(FSDirectory.open(indexDir));
      Version minSegVersion = segInfos.getMinSegmentLuceneVersion();

      log.trace("minSegVersion = {}", minSegVersion);

      // Run the IndexUpgrader tool if the minimum version is not on or after the latest version
      if (!minSegVersion.onOrAfter(Version.LATEST)) {
        new IndexUpgrader(FSDirectory.open(indexDir)).upgrade();
      } else {
        log.trace("IndexUpgrader not necessary [minVersion: {}, latestVersion: {}]", minSegVersion, Version.LATEST);
      }
    }

    private static final int LATEST_CONFIGSET_VERSION = 2;

    /**
     * Update the configuration set of a LOCKSS repository Solr core.
     */
    public void updateConfigSet() throws IOException {
      // Determine current version
      int currentVersion = getConfigSetVersion();
      int targetVersion = LATEST_CONFIGSET_VERSION;


      // Update index to target version iteratively
      for (int version = currentVersion; version < targetVersion; version++) {
        // Install configuration set
        installConfigSetVersion(version + 1);

        // Perform post-installation tasks
        postInstallConfigSet(version + 1);
      }
    }

    public void postInstallConfigSet(int targetVersion) {
      try {
        switch (targetVersion) {
          case 1:
            // NOP
            break;

          case 2:
            postInstallConfigSetFrom1to2();
            break;

          default:
            throw new IllegalArgumentException("No post configset installation tasks for version: " + targetVersion);
        }
      } catch (IOException | SolrServerException | SolrResponseErrorException e) {

      }
    }

    private void postInstallConfigSetFrom1to2() throws IOException, SolrServerException, SolrResponseErrorException {
      try (EmbeddedSolrServer solrClient = new EmbeddedSolrServer(solrHome, solrCoreName)) {
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

      } catch (IOException | SolrServerException | SolrResponseErrorException e) {
        String errorMessage = "Caught exception while performing post configset installation tasks for version 2";
        log.error(errorMessage, e);
        throw e;
      }
    }

    private Map<String, Object> getFieldModifier(String modifier, Object value) {
      Map<String, Object> fieldModifier = new HashMap<>();
      fieldModifier.put(modifier, value);
      return fieldModifier;
    }

    public static final String CONFIGOVERLAY_FILE = "configoverlay.json";
    public static final String CONFIGOVERLAY_USERPROPS_KEY = "userProps";
    public static final String LOCKSS_CONFIGSET_VERSION_KEY = "lockss-configset-version";

    /**
     * Determines the version of a LOCKSS Solr configuration set by reading its configuration overlay file and returning
     * the value of the "lockss-configset-version" key.
     * <p>
     * Returns 0 if the configuration overlay file could not be found, or the LOCKSS configuration set version key does
     * not exist in the overlay.
     *
     * @return An {@code int} containing the version of the configuration set.
     * @throws IOException
     */
    public int getConfigSetVersion() throws IOException {
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
     * Installs a version of a LOCKSS configuration set in this Solr core.
     *
     * @param version An {@code int} containing the LOCKSS configuration set version to install.
     */
    public void installConfigSetVersion(int version) throws IOException {
      Path srcPath = Paths.get(String.format("src/main/resources/solr/configsets/lockss/v%d/conf", version));
      installConfigSet(srcPath);
    }

    /**
     * Installs a configuration set into this Solr core provided the path to a configuration set.
     *
     * @param configSetPath
     * @throws IOException
     */
    public void installConfigSet(Path configSetPath) throws IOException {
      if (coreUsesConfigSet()) {
        // YES: Update configuration set in configset base directory (as specified in solr.xml)
        log.trace("Updating Solr configuration set [configSet: {}, configSetBaseDir: {}]", configSet, configSetBaseDir);

        // Determine config set path
        configDir = SolrXmlConfig.fromSolrHome(solrHome).getConfigSetBaseDirectory().resolve(configSet);
      } else {
        // NO: Update core's config directory
        log.trace("Updating Solr configuration [configDir: {}]", configDir);
      }

      log.trace("src = {}", configSetPath);
      log.trace("dst = {}", configDir);

      // Replace core's config set
      FileUtils.deleteDirectory(configDir.toFile());
      FileUtils.copyDirectory(configSetPath.toFile(), configDir.toFile());
    }

    /**
     * Returns a boolean indicating whether this Solr core uses a common configuration set.
     *
     * @return A {@code boolean} indicating whether this Solr core uses a common configuration set.
     */
    private boolean coreUsesConfigSet() {
      return configSet != null;
    }

    /**
     * Applies schema updates to this Solr core.
     */
    public void applySchemaUpdates() {
      try (SolrClient solrClient = new EmbeddedSolrServer(solrHome, solrCoreName)) {
        new ArtifactIndexSchemaUpdater(solrClient).updateSchema(TARGET_SCHEMA_VERSION);
      } catch (IOException | SolrServerException | SolrResponseErrorException e) {
        e.printStackTrace();
      }
    }

    /**
     * Experimental. Performs an in-place reindex of all documents in the Solr core. All fields must be stored in order
     * for this to have a chance of working.
     */
    public void reindexAllDocuments() {
      try (SolrClient solrClient = new EmbeddedSolrServer(solrHome, solrCoreName)) {
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
        e.printStackTrace();
      }
    }
  }

  /**
   * Returns a LocalCoreUpgrader for a Solr core under a Solr home directory.
   *
   * @param solrHome A {@code Path} to a Solr home directory.
   * @param coreName A {@code String} containing the name of the Solr core.
   * @return A {@code LocalCoreUpgrader} instance.
   */
  private static LocalCoreUpgrader getLocalCoreUpgrader(Path solrHome, String coreName) {
    return getLocalCoreUpgraders(solrHome).get(coreName);
  }

  /**
   * Builds a map of Solr cores under a Solr home directory to their LocalCoreUpgrader instances.
   *
   * @param solrHome A {@code Path} to a Solr home directory.
   * @return A {@code Map<String, LocalCoreUpgrader>} map from Solr core names to their LocalCoreUpgrader instance.
   */
  private static Map<String, LocalCoreUpgrader> getLocalCoreUpgraders(Path solrHome) {
    CoreContainer container = CoreContainer.createAndLoad(solrHome);

    Map<String, LocalCoreUpgrader> localUpdaters = container.getCores().stream().collect(
        Collectors.toMap(
            core -> core.getName(),
            core -> new LocalCoreUpgrader(core)
        )
    );

    container.shutdown();

    return localUpdaters;
  }

  // SOLR STANDALONE ///////////////////////////////////////////////////////////////////////////////////////////////////

  public static void createCore(SolrClient solrClient) throws IOException, SolrServerException {
    // Does core exist?
    CoreStatus status = CoreAdminRequest.getCoreStatus("xyzzy", solrClient);
    log.debug("status = {}", status.getInstanceDirectory());

    // Create a new core
    CoreAdminRequest.Create req = new CoreAdminRequest.Create();
    req.setCoreName("lockss-repo.new");
    req.setInstanceDir("lockss-repo.new");
    req.setConfigSet("_default");
    req.process(solrClient);
  }

  /**
   * Returns the names of the configuration sets available on this Solr instance.
   *
   * @param solrClient A {@code SolrClient} instance to get the list of configuration set names from.
   * @return A {@code List<String>} containing the names of the configuration sets available.
   * @throws IOException
   * @throws SolrServerException
   */
  public List<String> getConfigSets(SolrClient solrClient) throws IOException, SolrServerException {
    ConfigSetAdminRequest.List listReq = new ConfigSetAdminRequest.List();
    return listReq.process(solrClient).getConfigSets();
  }

  public void miscCoreOperations(SolrClient solrClient) throws IOException, SolrServerException {
    // Try renaming the core
    CoreAdminRequest.renameCore("lockss-repo.old", "lockss-repo", solrClient);
    CoreAdminRequest.unloadCore("lockss-repo.new", true, true, solrClient);
    CoreAdminRequest.swapCore("lockss-repo", "lockss-repo.new", solrClient);
  }

  // SOLR CLOUD ////////////////////////////////////////////////////////////////////////////////////////////////////////

  public static final String SOLR_CONFIGSET_PATH = "target/test-classes/solr";
  public static final String SOLR_CONFIGSET_NAME = "testConfig";

  public static class RemoteCoreUpdater {
    SolrClient client;

    public RemoteCoreUpdater(SolrClient client) {
      this.client = client;
    }

    public static void uploadConfigSet(CloudSolrClient client) throws IOException, SolrServerException {
      // Upload new configuration set
      try (SolrZkClient zkClient = new SolrZkClient(client.getZkHost(), 10)) {
        zkClient.upConfig(Paths.get(SOLR_CONFIGSET_PATH), SOLR_CONFIGSET_NAME);
//    new ZkConfigManager().uploadConfigDir();
//    zkClient.create("/test", "byte".getBytes(), CreateMode.PERSISTENT, true);
      }
    }

    public void applySchemaUpdates() {
      try {
        new ArtifactIndexSchemaUpdater(client).updateSchema(TARGET_SCHEMA_VERSION);
      } catch (SolrResponseErrorException | SolrServerException | IOException e) {
        e.printStackTrace();
      }
    }
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

  /**
   * Returns a boolean indicating whether a collection exists in a Solr Cloud cluster.
   *
   * @param client
   * @param collectionName
   * @return
   * @throws IOException
   * @throws SolrServerException
   */
  public boolean collectionExists(CloudSolrClient client, String collectionName) throws IOException, SolrServerException {
    List<String> collections = CollectionAdminRequest.listCollections(client);
    return collections.contains(collectionName);
  }

  // QUESTIONS /////////////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Returns a boolean indicating whether a Solr upgrade was detected.
   *
   * @return
   */
  public static boolean isSolrUpgraded() {
    // XXX Do we still need this? It was only really necessary for determining whether we need to run IndexUpgrader but
    //     I was able to identify a better way, by determining the version of Lucene/Solr used to write the index.
    return true;
  }

  // UPDATE PATHS //////////////////////////////////////////////////////////////////////////////////////////////////////

  private static final String DEFAULT_SOLRCORE_NAME = "lockss";
  private static final Path LATEST_SOLRCORE_CONF = Paths.get("src/test/resources/solr/testcore/conf");

  private static void updateLocalSolrCore(String solrHomePath) throws IOException, SolrResponseErrorException, SolrServerException {
    LocalCoreUpgrader localCoreUpgrader = getLocalCoreUpgraders(Paths.get(solrHomePath)).get(DEFAULT_SOLRCORE_NAME);

    // if (core.solrVersion() < core.updateTarget.solrVersion())
    localCoreUpgrader.updateLuceneIndex();

    // if (configSetDir != null) || (configset.hasChanges())
    localCoreUpgrader.installConfigSet(LATEST_SOLRCORE_CONF);

    // if (configSet.hasSchemaUpdates() || lockssSchema.hasUpdates())
    localCoreUpgrader.applySchemaUpdates();
  }

  private static void updateRemoteSolrCore(SolrClient solrClient) throws IOException, SolrResponseErrorException, SolrServerException {
    RemoteCoreUpdater coreUpdater = new RemoteCoreUpdater(solrClient);

    // if (configSet.hasSchemaUpdates() || lockssSchema.hasUpdates())
    coreUpdater.applySchemaUpdates();
  }

  private static void updateSolrCloud(CloudSolrClient cloudClient) throws IOException, SolrServerException, SolrResponseErrorException {
    RemoteCoreUpdater coreUpdater = new RemoteCoreUpdater(cloudClient);

    // if (configSetDir != null)
    coreUpdater.uploadConfigSet(cloudClient);

    // if (configSet.hasSchemaUpdates() || lockssSchema.hasUpdates())
    coreUpdater.applySchemaUpdates();
  }

  // ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  public static void main(String[] args) throws ParseException, IOException, SolrResponseErrorException, SolrServerException {
    // Define command-line options
    Options options = new Options();
    options.addOption(null, "local", true, "Local path to Solr home directory");
    options.addOption(null, "remote", true, "Remote Solr REST API endpoint");
    CommandLine cmd = new PosixParser().parse(options, args);

    if (cmd.hasOption("local") && cmd.hasOption("remote")) {
      throw new IllegalArgumentException("Both local and remote may not be specified at the same time!");
    }

    if (cmd.hasOption("local")) {

      // Upgrade the Solr core
      updateLocalSolrCore(cmd.getOptionValue("local"));

    } else if (cmd.hasOption("remote")) {
      // Get Solr server URL from command-line argument
      String solrUrl = cmd.getOptionValue("remote");

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
    } else {
      throw new IllegalArgumentException("Nothing to do; solrBase or solrEndpoint must be specified");
    }
  }
}
