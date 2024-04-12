---
date: 2024-04-11
authors:
  - snazy
---

# projectnessie.org site reorg

The [projectnessie.org](https://projectnessie.org) web site became a bit hard to navigate and was missing a
functionality to show the documentation/reference for particular Nessie releases. The reorg also reduced the
amount of "tabs" from 9 to 5 and reorganized the content quite a bit, solving the issue that information is
where site visitors would expect it to be.
<!-- more -->

What we have now - and did not have before:

* [_Guides_](../../guides/index.md): A better organized collection of guides and general information about Nessie.
* [_Docs_](../../nessie-latest/index.md): Focused on Nessie releases and their corresponding docs. The earliest release
  that is covered by this is version 0.79.0. Upcoming releases will be included here as well. This section also contains
  the status of current development, aka nightly or snapshot. Release and server-upgrade notes are covered here as well.
* [_Downloads_](../../downloads/index.md): A "one stop" page showing the download options of the current release.
  Download
  options for older releases are covered by in the [_Docs_](../../nessie-latest/index.md) section.
* [_Downloads_](../../community.md): Nothing changed here - it was already quite simple ;)
* [_Blog_](../../blog/index.md): The blog section received a little polishing.

A bunch of files and folders have been moved around and links have changed, redirects ensure that existing (perma)links
still work and are redirected to the new location.

On top of the site-reorg, a couple of minor issues have been fixed.

## Versioned docs

Versioned docs are not particularly rocket science, especially since other OSS projects do that already, and we could "
borrow" a couple of things. Overall, it was neither too difficult nor too time-consuming to add this useful feature.

Conceptually, versioned docs are "archived" in
a [separate branch](https://github.com/projectnessie/nessie/tree/site-docs) on GitHub. We plan to add more version
dependent documentation, potentially server configuration generated from source and maybe javadocs - that is why there
is [another branch](https://github.com/projectnessie/nessie/tree/site-javadoc), which is meant to hold "already
rendered" (think: HTML) per Nessie release.

Docs for the "latest in-development"
are [outside the mkdocs `docs/` tree](https://github.com/projectnessie/nessie/tree/main/site/in-dev/), because those
are rather templates than "ready to use" markdown files. Although, the term "template" is not exactly correct, but
the `site/in-dev/` folder serves as the source for the [Unreleased (nightly)](../../nessie-nightly/index.md) and the
contents are also used for Nessie releases. During a release, the "nightly `index.md`" is replaced by
the `index-release.md` - so there is some very rudimentary but sufficient templating at play.

Versioned docs are not added to the Nessie main source tree, because that would make the main source tree unnecessarily
grow. But that concept requires some scripting before the static site can be built. Those scripting is wrapped in a
very simple [`Makefile`](https://github.com/projectnessie/nessie/tree/main/site/Makefile). There are basically two
targets:

```bash
make build
```

and

```bash
make serve
```

Neither requires you to setup any Python virtual environment or install any dependency except the Python interpreter.
The scripts triggered by these `Makefile` targets take care of all the boilerplate.

## Strict is good

Notice that we build the site using the `strict: true` setting
in [`mkdocs.yml`](https://github.com/projectnessie/nessie/tree/main/site/mkdocs.yml). This mostly ensures that all links
work. And TBH this is also a reason why
the [`site/in-dev/`](https://github.com/projectnessie/nessie/tree/main/site/in-dev/) folder is not inside
the [`site/docs/`](https://github.com/projectnessie/nessie/tree/main/site/docs/) folder, because that breaks a "strict
build".

## Notes on IDEs

While IDEs became pretty smart wrt link highlighting and suggestions/autocompletion, links to and from `in-dev/` are
potentially highlighted as "broken" and suggestions are wrong - the IDEs just do not know how all that plays together.
The escape and solution: Use `make build`/`make serve` and watch the output.
