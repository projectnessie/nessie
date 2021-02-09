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
        .withUri("http://127.0.0.1:19120/api/v1")
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

## Gatling links

* [Intro to Gatling](https://www.baeldung.com/introduction-to-gatling)
* [Quickstart](https://gatling.io/docs/gatling/tutorials/quickstart/)
* [Advanced Tutorial](https://gatling.io/docs/gatling/tutorials/advanced/)
