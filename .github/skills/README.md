# Skills 🛠️

Each skill provides structured instructions for common development workflows.

## 📄 References

- [The Complete Guide to Building Skills](https://resources.anthropic.com/hubfs/The-Complete-Guide-to-Building-Skill-for-Claude.pdf)
- [Ralph Wiggum as a "software engineer"](https://ghuntley.com/ralph/)

## 💬 Human-in-the-loop

For regular tasks that require back-and-forth design discussions — use `/fleet`.

Example:

```bash
/fleet @.github/skills/ralph-dbt-scope/skill.md
```

## 🔁 Ralph Loop

We run a skill autonomously in a loop (re-invoking Copilot until the task is complete).

This is a Ralph loop.

It allows the Agent to make simple-but-useful changes until a desired state is reached.

### Start Ralph

```bash
cd ~/dbt-scope
.scripts/ralph.sh .github/skills/ralph-dbt-scope/skill.md --iterations 10
```

All Ralph skills should be constructed with skippability in mind (e.g. Step 1 - N).

This allows you to guide Ralph towards skipping easier/faster tests if you're confident it already worked:

```bash
.scripts/ralph.sh .github/skills/ralph-dbt-scope/skill.md --iterations 10 --skip-to "Do Step 1-2 only and skip Step 3+"
.scripts/ralph.sh .github/skills/ralph-dbt-scope/skill.md --iterations 10 --skip-to "Do Step 3+, since 1-2 was done already"
```

> 💡 The other alternative is to split the skills apart, this is difficult since there might be duplicate context in each.

### Available Ralph Skills

| Skill                                         | Description                                                                                |
| --------------------------------------------- | ------------------------------------------------------------------------------------------ |
| [`ralph-dbt-scope`](ralph-dbt-scope/skill.md) | Full regression loop: venv → install → build → lint → unit-test → debug → integration-test |

---

[Home](../../README.md) > [Skills](./)
