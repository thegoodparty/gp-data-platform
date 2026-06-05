"""People-API loader: Databricks -> S3 -> Aurora refresh pipeline.

This package is the first concrete consumer of the generic `loader.core`
harness. Future consumers would mirror this structure under their own
package (e.g. `loader.donor_api`) and register their own subcommands.
"""
