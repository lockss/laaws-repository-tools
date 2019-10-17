# Introduction


# Solr Update

It is possible to upgrade between major versions of Solr. Part of the upgrade process requires updating the Lucene index
of existing cores, using the IndexUpdater tool provided by Solr. The Solr administrator is expected to have followed 
these steps prior to using the SolrArtifactIndexUpdater tool if running the Solr server in standalone mode or as a Solr 
Cloud cluster. 

Alternatively, while the Solr server is offline, it is possible to point SolrArtifactIndexUpdater directly to a Solr 
home directory which will invoke the Lucene IndexUpdater tool on the administrator's behalf. If the Solr administrator
has already run the IndexUpdater tool against the core, this step is a no-op.

# Configuration Set Update

If there is an update available to the configuration set provided by LOCKSS, it must be made available to the Solr 
server by the administrator. For Solr server's running in standalone mode, the configuration set must be placed in the 
configsets directory under the Solr home directory, or replace the configuration directory within an existing Solr core.

If the Solr server is running as part of a Solr Cloud cluster, the configuration set must be made available to nodes of
the Solr cluster by uploading the configuration set to the underlying ZooKeeper cluster. This can be done using the Solr
command line tools or using the Solr provided ZooKeeper Configuration Manager API.

## Schema Updates

Updates to the configuration set may include changes to the Solr schema in order to define new field types, fields, 
analyzers, tokenizers, etc. Changes to the schema require re-indexing content. Normally this is performed using the 
content that was used the build the index in the first place, but it is possible to do an in-place reindex if the 
affected fields are stored.

The SolrArtifactIndexUpdater tool aims to provide the necessary transformations between versions of the schema.

### Solr Stand-alone 

1. Make a backup of the Solr server's home directory.
1. Upgrade Solr and run IndexUpdater on existing Lucene indexes if necessary.
1. Place new configuration set in the Solr server's home directory.
1. Run SorlArtifactIndexUpdater on existing core to re-index Artifacts.

### Solr Cloud

1. Make a backup of the Solr collection.
2. Upload the new configuration set to Solr Cloud cluster's ZooKeeper instance (either by directly modifying ZooKeeper 
   or using Solr's zkClient).
3. On each Solr node:
    1. Upgrade Solr and run IndexUpdater on existing Lucene indexes if necessary.
    1. Run SorlArtifactIndexUpdater on existing core to re-index Artifacts.

# Developer Notes

# Support

 Contact LOCKSS support at support@lockss.org