---
search:
  exclude: true
---
<!--start-->

Configuration for S3 compatible object stores. 

Default settings to be applied to all buckets can be set in the `default-options` group.  Specific settings for each bucket can be specified via the `buckets` map.   

All settings are optional. The defaults of these settings are defined by the AWSSDK Java  client.

