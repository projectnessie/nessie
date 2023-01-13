# Gatling Simulations for Nessie

Each simulation shall consist of a Scala case-class holding the simulation parameters and
the actual simulation class.

Simulation classes must extend `io.gatling.core.scenario.Simulation.Simulation`,
parameter case-classes should leverage `org.projectnessie.perftest.gatling.BaseParams`.

The Gatling Simulations for Nessie use the Nessie DSL/protocol implemented in the 
`nessie-perftest-gatling` module.

## Nessie-Gatling simulation tutorial

The main thing a Nessie-Gatling simulation needs is the setup code.

```scala
class SimpleSimulation extends Simulation {

  // Construct an instance of the SimulationParams, taking the parameters from
  // the system properties is the easiest way.
  val params: SimulationParams = SimulationParams.fromSystemProperties()

  // Construct the `NessieProtocol`, which will provide the given `NessieClient`
  // to the actions/executions.
  val nessieProtocol: NessieProtocol = nessie()
    .prometheusPush(params.setupPrometheusPush)
    .client(
      NessieClient
        .builder()
        .fromSystemProperties()
        .withTracing(params.prometheusPushURL.isDefined)
        .build()
    )

  // Simple scenario to get the default branch N times.
  val scenario = scenario("Get-Default-Branch-100-Times")
    .repeat(params.gets, exec(
      nessie(s"Get main branch")
        .execute { (client, session) =>
          val defaultBranch = client.getTreeApi.getDefaultBranch
          session.set("defaultBranch", defaultBranch)
        }))

  // Let the simulation run the scenario above for the configured number of users.
  val s: SetUp = setUp(scenario.inject(atOnceUsers(params.numUsers)))

  // Returns the "finished" `SetUp` with the `nessieProtocol` back to Gatling.
  // (Note: yes, this is is a "return statement".) 
  s.protocols(nessieProtocol)
}

// Case class with all the options/parameters your simulation needs.
case class SimulationParams(
                             gets: Int,
                             override val numUsers: Int,
                             override val opRate: Double,
                             override val prometheusPushURL: Option[String],
                             override val note: String
                           ) extends BaseParams

// Object for the case class - to provide the `fromSystemProperties()` function.
object SimulationParams {
  def fromSystemProperties(): SimulationParams = {
    val base = BaseParams.fromSystemProperties()
    val gets: Int = Integer.getInteger("sim.gets", 100).toInt
    SimulationParams(
      gets,
      base.numUsers,
      base.opRate,
      base.prometheusPushURL,
      base.note
    )
  }
}
```

## Gatling

Gatling simulations define scenarios. A scenario is a sequence of potentially nested and conditional
executions. You can think of a scenario ~= a use-case, something like "a user logs into a shopping
website, looks through the avaialable products, adds some items to the basket, checks out & pays".

Such scenarios often don't run "as fast as possible", but have some pauses and/or conditions
and/or repetitions.

Simulations can then use those scenarios and simulate many users running those scenarios, with
various options to ramp-up the number of users, ramp-down, etc.

Important note: The Gatling `Session` object is _immutable_! This means, when you add some attribute
to the `Session`, you receive a cloned object, which needs to be passed downstream in your code or,
as in most cases, returned form the actions' code.

## Running a single Nessie Gatling simulation

Gatling's Gradle plugin recognizes tasks in the form `gatlingRun-<fqcn>`, where `<fqcn>` is a dot-separated package-and-classname.  For instance, to run `KeyListSpillingSimulation` from the top-level project directory, execute:

```
./gradlew :nessie-perftest-simulations:gatlingRun-org.projectnessie.perftest.gatling.KeyListSpillingSimulation

```

The simluations in this package take most configuration as JVM system properties passed to the `gradlew` wrapper command.  An example follows.  The `sim.` prefix on properties is pure convention.  Simulations could define properties with arbitrary names.

```bash
./gradlew :nessie-perftest-simulations:gatlingRun-org.projectnessie.perftest.gatling.KeyListSpillingSimulation \
	-Dsim.putsPerBaseCommit=1000 \
	-Dsim.duration.seconds=60 \
	-Dsim.putsPerTestCommit=1
```

This works for other system properties related to client configuration.  For instance, to point the Gatling simulation's Nessie client at a different backend than the Quarkus instance started by the task's dependencies, pass `-Dnessie.uri=...`, e.g.:

```bash
./gradlew :nessie-perftest-simulations:gatlingRun-org.projectnessie.perftest.gatling.KeyListSpillingSimulation \
	-Dnessie.uri=http://127.0.0.1:19120/api/v1
```

The system property `gatling.logLevel` is also specifically checked in build.gradle.kts.  When `-Dgatling.logLevel=...` is passed to the `gradlew` wrapper command, its value will be configured as the logLevel parameter on Gatling's Gradle plugin, changing what Gatling and the simulation logs.  This has no effect on the Nessie/server side.  This check is unique to `gatling.logLevel`; the plugin has other parameters that can't be configured this way, as of the time of writing.

```bash
./gradlew :nessie-perftest-simulations:gatlingRun-org.projectnessie.perftest.gatling.KeyListSpillingSimulation \
	-Dgatling.logLevel=DEBUG
```

## Running a simulation against an external Nessie server

A local Nessie server is started and used for Gatling runs, when running a `gatlingRun` Gradle task.
To use an external Nessie server, pass the system property `nessie.uri` to Gradle, for example like
this:

```bash
./gradlew \
  -Dnessie.uri=http://127.0.0.1:19120/api/v2 \
  :nessie-perftest-simulations:gatlingRun-org.projectnessie.perftest.gatling.CommitToBranchSimulation
```

System properties handling when using an external Nessie server:
* System properties starting with `nessie.` will be passed to the Gatling simulation(s) as is. This
  allows configuration of the Nessie client and to set additional Nessie client options.
* System properties starting with `gatling.` will be passed to the Gatling simulation, removing
  `gatling.` from the key. For example: using `./gradlew -Dgatling.foo.bar=baz ...` will pass the
  system property `foo.bar=baz` to the Gatling simulation(s).
* The values of system properties starting with `gatling.jvmArg` are passed to the Gatling
  simulations. For example: using `./gradlew -Dgatling.jvmArg.x=-Xmx4g ...` will pass the JVM arg
  `-Xmx4g` to the Gatling simulation(s).

## Gatling links

* [Intro to Gatling](https://www.baeldung.com/introduction-to-gatling)
* [Quickstart](https://gatling.io/docs/gatling/tutorials/quickstart/)
* [Advanced Tutorial](https://gatling.io/docs/gatling/tutorials/advanced/)
