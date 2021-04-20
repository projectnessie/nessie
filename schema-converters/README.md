# Nessie Schema Converters

This directory contains the modules which convert the internal Nessie schema representation to and from well known schema
formats in other engies. The modules typically only provide a single public class with static methods to convert `to` and
`from` the desired schema.

## Currently supported Schema types

* Arrow: Arrow is the base format for the internal Nessie Schema format so the conversion is simple and almost 1:1.
* Iceberg: **TODO**
