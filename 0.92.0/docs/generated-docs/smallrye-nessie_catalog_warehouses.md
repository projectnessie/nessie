---
search:
  exclude: true
---
<!--start-->

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `nessie.catalog.warehouses.warehouses.`_`<warehouse-name>`_ |  | `` | Map of warehouse names to warehouse configurations.  |
| `nessie.catalog.warehouses.warehouses.`_`<warehouse-name>`_`.iceberg-config-defaults.`_`<iceberg-property>`_ |  | `string` | Iceberg config defaults specific to this warehouse, potentially overriding any defaults  specified in `iceberg-config-defaults` in [Warehouse  defaults ](#warehouse-defaults).  |
| `nessie.catalog.warehouses.warehouses.`_`<warehouse-name>`_`.iceberg-config-overrides.`_`<iceberg-property>`_ |  | `string` | Iceberg config overrides specific to this warehouse. They override any overrides specified in  `iceberg-config-overrides` in [Warehouse defaults](#warehouse-defaults). |
| `nessie.catalog.warehouses.warehouses.`_`<warehouse-name>`_`.location` |  | `string` | Location of the warehouse. Used to determine the base location of a table. |
