---
search:
  exclude: true
---
<!--start-->

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.default-warehouse` |  | `string` | Name of the default warehouse. This one is used when a warehouse is not specified in a query.  If no default warehouse is configured and a request does not specify a warehouse, the request  will fail.  |
| `nessie.catalog.warehouses.`_`<warehouse-name>`_ |  | `` | Map of warehouse names to warehouse configurations.  |
| `nessie.catalog.warehouses.`_`<warehouse-name>`_`.iceberg-config-defaults.`_`<iceberg-property>`_ |  | `string` | Iceberg config defaults specific to this warehouse. They override any defaults specified in  (`CatalogConfig#icebergConfigDefaults()`). |
| `nessie.catalog.warehouses.`_`<warehouse-name>`_`.iceberg-config-overrides.`_`<iceberg-property>`_ |  | `string` | Iceberg config overrides specific to this warehouse. They override any overrides specified in  (`CatalogConfig#icebergConfigOverrides()`). |
| `nessie.catalog.warehouses.`_`<warehouse-name>`_`.location` |  | `string` | Location of the warehouse. Used to determine the base location of a table. |
| `nessie.catalog.iceberg-config-defaults.`_`<iceberg-property>`_ |  | `string` | Iceberg config defaults applicable to all clients and warehouses. Any properties that are  common to all iceberg clients should be included here. They will be passed to all clients on  all warehouses as config defaults. These defaults can be overridden on a per-warehouse basis,  see (`WarehouseConfig#icebergConfigDefaults()`). |
| `nessie.catalog.iceberg-config-overrides.`_`<iceberg-property>`_ |  | `string` | Iceberg config overrides applicable to all clients and warehouses. Any properties that are  common to all iceberg clients should be included here. They will be passed to all clients on  all warehouses as config overrides. These overrides can be overridden on a per-warehouse basis,  see (`WarehouseConfig#icebergConfigOverrides()`). |
