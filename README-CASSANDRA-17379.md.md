CCM (Cassandra Cluster Manager)
CASSANDRA-17379 README
====================================================


---- 
 


WARNING - CCM configuration changes using updateconf does not happen according to CASSANDRA-17379
-------------------------------------------------------------------------------------------------

After CASSANDRA-15234, to support the Python upgrade tests CCM updateconf is replacing
new key name and value in case the old key name and value is provided.
For example, if you add to config `permissions_validity_in_ms`, it will replace
`permissions_validity` in default cassandra.yaml
This was needed to ensure correct overloading as CCM cassandra.yaml has keys
sorted lexicographically. CASSANDRA-17379 was opened to improve the user experience
and deprecate the overloading of parameters in cassandra.yaml. In CASSANDRA 4.1+, by default,
we refuse starting Cassandra with a config containing both old and new config keys for the
same parameter. Start Cassandra with `-Dcassandra.allow_new_old_config_keys=true` to override.
For historical reasons duplicate config keys in cassandra.yaml are allowed by default, start
Cassandra with `-Dcassandra.allow_duplicate_config_keys=false` to disallow this. Please note
that key_cache_save_period, row_cache_save_period, counter_cache_save_period will be affected
only by `-Dcassandra.allow_duplicate_config_keys`. Ticket CASSANDRA-17949 was opened to decide
the future of CCM updateconf post CASSANDRA-17379, until then - bear in mind that old replace
new parameters' in cassandra.yaml when using updateconf even if
`-Dcassandra.allow_new_old_config_keys=false` is set by default.

TLDR Do not exercise overloading of parameters in CCM if possible. Also, the mentioned changes
are done only in master branch. Probably the best way to handle cassandra 4.1 in CCM at this
point is to set `-Dcassandra.allow_new_old_config_keys=false` and
`-Dcassandra.allow_duplicate_config_keys=false`
to prohibit any kind of overloading when using CCM master and CCM released versions
