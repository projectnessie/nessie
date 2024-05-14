"""
This file is enabled by mkdocs-macros-plugin: https://mkdocs-macros-plugin.readthedocs.io/en/latest/
...and is directly targetted in the plugins.macros.module_name key of the mkdocs.yml configuration.
"""

def define_env(env):
    """
    This is the hook for defining variables, macros and filters.

    - variables: the dictionary that contains the environment variables (extra in mkdocs.yml).
    - macro: a decorator function, to declare a macro.  Will be universally available.
    - filter: a function with one of more arguments, used to perform a transformation.  Will be universally available.
    """ 

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

    __latest_iceberg_version = env.variables.versions['iceberg']

    @env.macro
    def iceberg_spark_runtime(spark="3.3", scala="2.12", version=__latest_iceberg_version):
        return __maven_artifact(
            "org.apache.iceberg",
            f"iceberg-spark-runtime-{spark}_{scala}",
            version
        )

    __latest_nessie_version = env.variables.versions['nessie']

    @env.macro
    def nessie_spark_extensions(spark="3.3", scala="2.12", version=__latest_nessie_version):
        return __maven_artifact(
            "org.projectnessie.nessie-integrations",
            f"nessie-spark-extensions-{spark}_{scala}",
            version
        )
