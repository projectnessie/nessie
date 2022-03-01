## Dev

* Run `./mvnw quarkus:dev` to start the quarkus server
* Run `target/node/npm test -- --coverage --watchAll` to get unit tests constantly running. This has a mini console which can be used to re-run tests or to re-create jest snapshots
* Run `target/node/npm start --scripts-prepend-node-path` to start up a server and open a browser w/ the UI constantly updating

I use `pre-commit` to run `prettier` and `eslint` as part of the pre-commit hook and checks.

## Tools

* prettier - format text
* eslint - find common bugs and such
* jest - testing framework. We use snapshots heavily
* testing-library - the tool to help test React w/ async, events and DOM changes etc
* nock - stub out REST calls
* We use React functional components and hooks instead of classes/objects
* We use a proxy which forwards a running nessie on port 19120 to the port where `npm run` runs the webserver (3000) avoids having to deal w/ xss

