# Project Manager Agent

Project Manager is a Coordinator agent living in `agents/pm`.

Its goal is to fulfil the development plan by coordinating coding agents to strike cards one by one,
moving developemnt cards from TODO stage to DONE stage.

## Synopsis

Elaborate about the agent...

## Methodology

A kanban directory structure is in place under `agents/pm/kanban`, where file cards are moved
sequentially between columns (todo, doing, in_review, discarded, ready, landed).

New cards land in `agents/kanban/todo` under the strict contract `agents/pm/resources/CARD_TEMPLATE.md`,
with its own id, priority, blocked by fields.

Once a card is picked, it moves from `agents/pm/kanban/todo` to `agents/pm/kanban/doing` and a coding
agent spawns a thread to resolve the card in a dedicated worktree.
If the worker is interrupted, it can pick up from doing in the named worktree.

Next up is moving to In Review (`agents/pm/kanban/in_review`) stage.
Worktree should be rebased against `main`, a sanity check is performed with the current project state,
tests should pass, code should be linted.
These topics should be clearly resolved before moving the card to Ready (`agents/pm/kanban/ready`) stage.

Following up is moving to Ready (`agents/pm/kanban/ready`) stage.
At this stage cards are treated in groups rather than individually.
A few related cards may form a mergeable group (one or more cards).
The mergeable group of cards (worktrees) is then merged against a new transient branch,
which should resolve conflicts, rebase against main, pass test suite, lint code, and pass a sanity check.
Squash per-card commits for a reasonable, sensible git history hygiene.
Once all these topics are resolved, the branch can then be merged into `main`, finally moving cards to
their final stage done (`agents/pm/kanban/done`).

At any stage, if a task is deemed no longer relevant or superseded, it must be moved to final stage
Discarded (`agents/pm/kanban/discarded`).

## Resolution order

Because a card may block another card, imposing a strict resolution order is paramount.

As a resolution strategy, maintain a dependency chart (`agents/pm/resources/depchart.yml`) to feed it
to a topological sort (`agents/pm/resources/topo_sort.py`).
