"""
This file is enabled by mkdocs-macros-plugin: https://mkdocs-macros-plugin.readthedocs.io/en/latest/
...and is directly targetted in the plugins.macros.module_name key of the mkdocs.yml configuration.
"""

import json

def define_env(env):
    """
    This is the hook for defining variables, macros and filters.

    - variables: the dictionary that contains the environment variables (extra in mkdocs.yml).
    - macro: a decorator function, to declare a macro.  Will be universally available.
    - filter: a function with one of more arguments, used to perform a transformation.  Will be universally available.
    """

    with open('iceberg_nessie_spark_versions.json') as f:
      __iceberg_nessie_spark_versions = json.load(f)

    def __maven_artifact(group, artifact, version):
        """
        Create a package of useful things about a Maven artifact.
        Follows Maven naming conventions: https://maven.apache.org/guides/mini/guide-naming-conventions.html
        """
        group_slashified = group.replace('.', '/')
        return {
            'spark_jar_package': f'{group}:{artifact}:{version}',
            'jar_url': f'https://repo.maven.apache.org/maven2/{group_slashified}/{artifact}/{version}/{artifact}-{version}.jar',
            'all_versions_url': f"https://search.maven.org/artifact/{group}/{artifact}"
        }

    @env.macro
    def spark_versions():
      return __iceberg_nessie_spark_versions["sparkVersions"]

    __latest_spark_version = spark_versions()[0]

    @env.macro
    def spark_scala_versions(spark=__latest_spark_version):
      return __iceberg_nessie_spark_versions["sparkVersion-" + spark + "-scalaVersions"]

    @env.macro
    def iceberg_versions():
        return __iceberg_nessie_spark_versions["iceberg_nessie"].keys()

    @env.macro
    def nessie_iceberg_version(iceberg_version):
        return __iceberg_nessie_spark_versions["iceberg_nessie"][iceberg_version]

    __latest_iceberg_version = env.variables.versions['iceberg']

    @env.macro
    def iceberg_spark_runtime(spark=__latest_spark_version, iceberg_version=__latest_iceberg_version, scala="2.12"):
        return __maven_artifact(
            "org.apache.iceberg",
            f"iceberg-spark-runtime-{spark}_{scala}",
          iceberg_version
        )

    __latest_nessie_in_latest_iceberg = nessie_iceberg_version(__latest_iceberg_version)

    @env.macro
    def nessie_spark_extensions(spark=__latest_spark_version, nessie_version=__latest_nessie_in_latest_iceberg, scala="2.12"):
        return __maven_artifact(
            "org.projectnessie.nessie-integrations",
            f"nessie-spark-extensions-{spark}_{scala}",
          nessie_version
        )

    @env.macro
    def nessie_spark_extensions_by_iceberg(spark=__latest_spark_version, iceberg_version=__latest_iceberg_version, scala="2.12"):
        return nessie_spark_extensions(spark, nessie_iceberg_version(iceberg_version), scala)
