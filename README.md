### DHS cluster conflicts

This project takes cluster data from DHS surveys and combines with child soldier, girl child soldier, and sexual violence data alongside conflict from UCDP GED as defined by the source inputs

### Dependencies

Python models are used for geodata processing. These are run locally using the dbt-fal adapter. SQL models are run on google BigQuery. The size of this project should very easily remain within the free monthly quotas for BQ if used with sanity.

Python dependencies:
- dbt-bigquery
- db-dtypes
- dbt-fal

Python model dependencies:
- pandas
- country-converter
- GeoPandas (this has underlying C++ dependencies such as PROJ and GEOS)

### dbt profile config

Your dbt profile, which by default is located at ~/.dbt/profiles.yml, should look something like this:

```
dhs_dbt_bq:
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: [project-name]
      dataset: dhs
      thread: 1
      keyfile: [/path/to/key.json]
    dev_with_fal:
      type: fal
      db_profile: dev
  target: dev_with_fal
```

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
